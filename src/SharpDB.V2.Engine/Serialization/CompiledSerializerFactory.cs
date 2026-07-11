using System.Buffers;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace SharpDB.V2.Engine.Serialization;

/// <summary>Reads an entire encoded record from <paramref name="data"/> and builds a <typeparamref name="T"/>.</summary>
internal delegate T SpanReader<out T>(ReadOnlySpan<byte> data);

/// <summary>Computes <paramref name="value"/>'s exact encoded size, requests a span of that size from
/// <paramref name="writer"/>, and writes the encoded record into it in a single pass — every variable-length
/// field (string UTF-8 byte count, byte[] length) is measured exactly once and reused for both the size
/// calculation and the write, rather than measuring once for sizing and again while writing.</summary>
internal delegate void SerializeAction<in T>(T value, IBufferWriter<byte> writer);

internal enum FieldKind
{
    Int32, Int64, Double, Boolean, Decimal, Guid, DateTime, DateTimeOffset,
    NullableInt32, NullableInt64, NullableDouble, NullableBoolean, NullableDecimal, NullableGuid, NullableDateTime, NullableDateTimeOffset,
    String, ByteArray
}

internal sealed class TypePlan<T>
{
    public required Func<T, int> Measure { get; init; }
    public required SerializeAction<T> SerializeInto { get; init; }
    public required SpanReader<T> Read { get; init; }
}

/// <summary>
/// Builds and caches, once per closed generic <c>T</c>, the compiled encode/decode delegates
/// <see cref="BinarySerializer{T}"/> uses. Reflection (property discovery, constructor lookup) only
/// happens the first time a given <c>T</c> is requested; every <see cref="Serialize"/>/<see cref="Deserialize"/>
/// call afterward runs the compiled delegates with no <see cref="PropertyInfo"/> access at all.
/// Internal: the only sanctioned entry point to serialization is <see cref="ISerializer{T}"/>.
/// </summary>
internal static class CompiledSerializerFactory
{
    private static readonly ConcurrentDictionary<Type, Lazy<object>> Cache = new();

    public static TypePlan<T> GetOrBuild<T>()
    {
        // LazyThreadSafetyMode.ExecutionAndPublication caches a thrown exception and rethrows the
        // same instance on every subsequent access, instead of re-walking reflection each time for
        // a permanently-unsupported T (verified: LazyThreadSafetyMode.None/ExecutionAndPublication
        // both cache; only PublicationOnly retries the factory after a failure).
        var lazy = Cache.GetOrAdd(typeof(T), static _ => new Lazy<object>(BuildPlan<T>, LazyThreadSafetyMode.ExecutionAndPublication));
        return (TypePlan<T>)lazy.Value;
    }

    private static TypePlan<T> BuildPlan<T>()
    {
        var type = typeof(T);
        var ctor = type.GetConstructor(Type.EmptyTypes)
            ?? throw new NotSupportedException(
                $"Type '{type.Name}' has no accessible parameterless constructor; BinarySerializer<T> requires one to materialize instances during deserialization.");

        var properties = type
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite && p.GetIndexParameters().Length == 0)
            .OrderBy(p => p.MetadataToken)
            .ToArray();

        var kinds = new FieldKind[properties.Length];
        for (var i = 0; i < properties.Length; i++)
        {
            kinds[i] = ClassifyKind(properties[i].PropertyType)
                ?? throw new NotSupportedException(
                    $"Property '{type.Name}.{properties[i].Name}' of type '{properties[i].PropertyType}' is not supported by BinarySerializer<T>. " +
                    "Supported types: int, long, double, bool, decimal, Guid, DateTime, DateTimeOffset, string, byte[], and their Nullable<T> counterparts.");
        }

        // Presence-bearing fields (Nullable<T>/string/byte[]) share a single leading bitmask instead of
        // one presence byte each; presenceBitIndex[i] is field i's 0-based bit position in that mask,
        // or -1 for non-presence-bearing (always-present) fields.
        var presenceBitIndex = new int[properties.Length];
        var presenceBearingCount = 0;
        for (var i = 0; i < kinds.Length; i++)
            presenceBitIndex[i] = IsPresenceBearing(kinds[i]) ? presenceBearingCount++ : -1;
        var maskByteCount = (presenceBearingCount + 7) / 8;

        // A schema with no presence-bearing fields at all (only fixed-size, non-nullable value kinds)
        // needs no mask/offset arithmetic at runtime — every field's offset is already known at build
        // time, so it gets its own simpler, faster code-generation path instead of the generic one.
        if (presenceBearingCount == 0)
        {
            return new TypePlan<T>
            {
                Measure = BuildMeasureFixedOnly<T>(kinds),
                SerializeInto = BuildSerializeIntoFixedOnly<T>(properties, kinds),
                Read = BuildReadFixedOnly<T>(properties, kinds, ctor)
            };
        }

        return new TypePlan<T>
        {
            Measure = BuildMeasure<T>(properties, kinds, maskByteCount),
            SerializeInto = BuildSerializeInto<T>(properties, kinds, presenceBitIndex, maskByteCount),
            Read = BuildRead<T>(properties, kinds, presenceBitIndex, maskByteCount, presenceBearingCount, ctor)
        };
    }

    private static FieldKind? ClassifyKind(Type propertyType)
    {
        if (propertyType == typeof(int)) return FieldKind.Int32;
        if (propertyType == typeof(long)) return FieldKind.Int64;
        if (propertyType == typeof(double)) return FieldKind.Double;
        if (propertyType == typeof(bool)) return FieldKind.Boolean;
        if (propertyType == typeof(decimal)) return FieldKind.Decimal;
        if (propertyType == typeof(Guid)) return FieldKind.Guid;
        if (propertyType == typeof(DateTime)) return FieldKind.DateTime;
        if (propertyType == typeof(DateTimeOffset)) return FieldKind.DateTimeOffset;
        if (propertyType == typeof(string)) return FieldKind.String;
        if (propertyType == typeof(byte[])) return FieldKind.ByteArray;

        var underlying = Nullable.GetUnderlyingType(propertyType);
        if (underlying == typeof(int)) return FieldKind.NullableInt32;
        if (underlying == typeof(long)) return FieldKind.NullableInt64;
        if (underlying == typeof(double)) return FieldKind.NullableDouble;
        if (underlying == typeof(bool)) return FieldKind.NullableBoolean;
        if (underlying == typeof(decimal)) return FieldKind.NullableDecimal;
        if (underlying == typeof(Guid)) return FieldKind.NullableGuid;
        if (underlying == typeof(DateTime)) return FieldKind.NullableDateTime;
        if (underlying == typeof(DateTimeOffset)) return FieldKind.NullableDateTimeOffset;

        return null;
    }

    private static int FixedSizeOf(FieldKind kind) => kind switch
    {
        FieldKind.Int32 or FieldKind.NullableInt32 => FieldCodecs.Int32Size,
        FieldKind.Int64 or FieldKind.NullableInt64 => FieldCodecs.Int64Size,
        FieldKind.Double or FieldKind.NullableDouble => FieldCodecs.DoubleSize,
        FieldKind.Boolean or FieldKind.NullableBoolean => FieldCodecs.BooleanSize,
        FieldKind.Decimal or FieldKind.NullableDecimal => FieldCodecs.DecimalSize,
        FieldKind.Guid or FieldKind.NullableGuid => FieldCodecs.GuidSize,
        FieldKind.DateTime or FieldKind.NullableDateTime => FieldCodecs.DateTimeSize,
        FieldKind.DateTimeOffset or FieldKind.NullableDateTimeOffset => FieldCodecs.DateTimeOffsetSize,
        _ => 0 // String, ByteArray: variable-length, no fixed size
    };

    private static bool IsNullableValueKind(FieldKind kind) => kind is
        FieldKind.NullableInt32 or FieldKind.NullableInt64 or FieldKind.NullableDouble or FieldKind.NullableBoolean or
        FieldKind.NullableDecimal or FieldKind.NullableGuid or FieldKind.NullableDateTime or FieldKind.NullableDateTimeOffset;

    private static bool IsReferenceKind(FieldKind kind) => kind is FieldKind.String or FieldKind.ByteArray;

    private static bool IsPresenceBearing(FieldKind kind) => IsNullableValueKind(kind) || IsReferenceKind(kind);

    private static readonly MethodInfo ReadOnlySpanSlice = typeof(ReadOnlySpan<byte>).GetMethod("Slice", [typeof(int), typeof(int)])!;
    private static readonly MethodInfo SpanSlice = typeof(Span<byte>).GetMethod("Slice", [typeof(int), typeof(int)])!;

    private static (MethodInfo Write, MethodInfo Read) CodecMethods(FieldKind kind)
    {
        var name = kind switch
        {
            FieldKind.Int32 or FieldKind.NullableInt32 => "Int32",
            FieldKind.Int64 or FieldKind.NullableInt64 => "Int64",
            FieldKind.Double or FieldKind.NullableDouble => "Double",
            FieldKind.Boolean or FieldKind.NullableBoolean => "Boolean",
            FieldKind.Decimal or FieldKind.NullableDecimal => "Decimal",
            FieldKind.Guid or FieldKind.NullableGuid => "Guid",
            FieldKind.DateTime or FieldKind.NullableDateTime => "DateTime",
            FieldKind.DateTimeOffset or FieldKind.NullableDateTimeOffset => "DateTimeOffset",
            _ => throw new InvalidOperationException($"CodecMethods does not apply to {kind}")
        };
        var write = typeof(FieldCodecs).GetMethod($"Write{name}")!;
        var read = typeof(FieldCodecs).GetMethod($"Read{name}")!;
        return (write, read);
    }

    // ── Measure ──────────────────────────────────────────────────────────────

    private static Func<T, int> BuildMeasure<T>(PropertyInfo[] properties, FieldKind[] kinds, int maskByteCount)
    {
        var valueParam = Expression.Parameter(typeof(T), "value");
        var totalVar = Expression.Variable(typeof(int), "total");
        // Format version byte + shared presence bitmask (0 bytes if there are no presence-bearing fields).
        var body = new List<Expression> { Expression.Assign(totalVar, Expression.Constant(1 + maskByteCount)) };

        for (var i = 0; i < properties.Length; i++)
        {
            var kind = kinds[i];
            var propExpr = Expression.Property(valueParam, properties[i]);

            Expression contribution;
            if (IsNullableValueKind(kind))
            {
                var hasValue = Expression.Property(propExpr, "HasValue");
                contribution = Expression.Condition(hasValue, Expression.Constant(FixedSizeOf(kind)), Expression.Constant(0));
            }
            else if (kind == FieldKind.String)
            {
                var isNull = Expression.Equal(propExpr, Expression.Constant(null, typeof(string)));
                var measure = Expression.Call(typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.MeasureString))!, propExpr);
                var ifPresent = Expression.Add(Expression.Constant(FieldCodecs.LengthPrefixSize), measure);
                contribution = Expression.Condition(isNull, Expression.Constant(0), ifPresent);
            }
            else if (kind == FieldKind.ByteArray)
            {
                var isNull = Expression.Equal(propExpr, Expression.Constant(null, typeof(byte[])));
                var length = Expression.ArrayLength(propExpr);
                var ifPresent = Expression.Add(Expression.Constant(FieldCodecs.LengthPrefixSize), length);
                contribution = Expression.Condition(isNull, Expression.Constant(0), ifPresent);
            }
            else
            {
                // Non-nullable value fields carry no presence bit at all: they are always present.
                contribution = Expression.Constant(FixedSizeOf(kind));
            }

            body.Add(Expression.AddAssign(totalVar, contribution));
        }

        body.Add(totalVar);
        var block = Expression.Block(typeof(int), [totalVar], body);
        return Expression.Lambda<Func<T, int>>(block, valueParam).Compile();
    }

    // ── Serialize (merged measure + write, single pass) ──────────────────────
    // Combines what used to be separate Measure/Write delegates into one: every presence-bearing
    // field's value, present/absent flag, and (for string/byte[]) UTF-8/array length is read exactly
    // once and cached, then reused for both the total-size calculation and the actual write — instead
    // of BuildMeasure and BuildWrite each independently re-deriving (and, for strings, re-measuring)
    // the same information. EstimateSize is unaffected; it still uses the standalone BuildMeasure above.

    private static SerializeAction<T> BuildSerializeInto<T>(PropertyInfo[] properties, FieldKind[] kinds, int[] presenceBitIndex, int maskByteCount)
    {
        var valueParam = Expression.Parameter(typeof(T), "value");
        var writerParam = Expression.Parameter(typeof(IBufferWriter<byte>), "writer");
        var totalVar = Expression.Variable(typeof(int), "total");
        var spanVar = Expression.Variable(typeof(Span<byte>), "span");
        var offsetVar = Expression.Variable(typeof(int), "offset");

        var getSpanMethod = typeof(IBufferWriter<byte>).GetMethod(nameof(IBufferWriter<byte>.GetSpan))!;
        var advanceMethod = typeof(IBufferWriter<byte>).GetMethod(nameof(IBufferWriter<byte>.Advance))!;
        var writeByteMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.WriteByte))!;
        var writeInt32Method = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.WriteInt32))!;

        Expression SliceSpan(int length) => Expression.Call(spanVar, SpanSlice, offsetVar, Expression.Constant(length));
        Expression SliceSpanVar(Expression lengthExpr) => Expression.Call(spanVar, SpanSlice, offsetVar, lengthExpr);
        Expression AdvanceOffset(Expression amount) => Expression.AddAssign(offsetVar, amount);

        // Per presence-bearing field: its cached value (read from the property exactly once), its
        // present/absent flag, and (string/byte[] only) its already-computed variable length.
        var fieldValVars = new ParameterExpression?[properties.Length];
        var presentVars = new ParameterExpression?[properties.Length];
        var varLenVars = new ParameterExpression?[properties.Length];
        for (var i = 0; i < properties.Length; i++)
        {
            if (presenceBitIndex[i] < 0) continue;
            fieldValVars[i] = Expression.Variable(properties[i].PropertyType, $"fieldVal_{i}");
            presentVars[i] = Expression.Variable(typeof(bool), $"present_{presenceBitIndex[i]}");
            if (kinds[i] is FieldKind.String or FieldKind.ByteArray)
                varLenVars[i] = Expression.Variable(typeof(int), $"varLen_{i}");
        }

        var maskVars = new ParameterExpression[maskByteCount];
        for (var m = 0; m < maskByteCount; m++)
            maskVars[m] = Expression.Variable(typeof(int), $"mask_{m}");

        var body = new List<Expression>();

        // Pass 1: cache each presence-bearing field's value/presence/variable-length exactly once.
        for (var i = 0; i < properties.Length; i++)
        {
            if (presenceBitIndex[i] < 0) continue;
            var kind = kinds[i];
            body.Add(Expression.Assign(fieldValVars[i]!, Expression.Property(valueParam, properties[i])));

            Expression isPresent = IsNullableValueKind(kind)
                ? Expression.Property(fieldValVars[i]!, "HasValue")
                : Expression.NotEqual(fieldValVars[i]!, Expression.Constant(null, properties[i].PropertyType));
            body.Add(Expression.Assign(presentVars[i]!, isPresent));

            if (kind == FieldKind.String)
            {
                var measure = Expression.Call(typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.MeasureString))!, fieldValVars[i]!);
                body.Add(Expression.Assign(varLenVars[i]!, Expression.Condition(presentVars[i]!, measure, Expression.Constant(0))));
            }
            else if (kind == FieldKind.ByteArray)
            {
                var length = Expression.ArrayLength(fieldValVars[i]!);
                body.Add(Expression.Assign(varLenVars[i]!, Expression.Condition(presentVars[i]!, length, Expression.Constant(0))));
            }
        }

        // Pass 2: sum the exact total size (mirrors BuildMeasure's formula, reusing the cached locals —
        // no field is measured a second time here).
        body.Add(Expression.Assign(totalVar, Expression.Constant(1 + maskByteCount)));
        for (var i = 0; i < properties.Length; i++)
        {
            var kind = kinds[i];
            Expression contribution;
            if (presenceBitIndex[i] < 0)
                contribution = Expression.Constant(FixedSizeOf(kind));
            else if (IsNullableValueKind(kind))
                contribution = Expression.Condition(presentVars[i]!, Expression.Constant(FixedSizeOf(kind)), Expression.Constant(0));
            else // String, ByteArray
                contribution = Expression.Condition(
                    presentVars[i]!,
                    Expression.Add(Expression.Constant(FieldCodecs.LengthPrefixSize), varLenVars[i]!),
                    Expression.Constant(0));
            body.Add(Expression.AddAssign(totalVar, contribution));
        }

        // Pass 3: accumulate the shared presence bitmask.
        for (var m = 0; m < maskByteCount; m++)
            body.Add(Expression.Assign(maskVars[m], Expression.Constant(0)));
        for (var i = 0; i < properties.Length; i++)
        {
            var p = presenceBitIndex[i];
            if (p < 0) continue;
            var bit = Expression.Constant(1 << (p % 8));
            body.Add(Expression.IfThen(presentVars[i]!, Expression.AddAssign(maskVars[p / 8], bit)));
        }

        // Now that the exact size is known, request the destination span and write the header.
        body.Add(Expression.Assign(spanVar, Expression.Call(writerParam, getSpanMethod, totalVar)));
        body.Add(Expression.Assign(offsetVar, Expression.Constant(0)));
        body.Add(Expression.Call(writeByteMethod, SliceSpan(1), Expression.Constant(FieldCodecs.FormatVersion)));
        body.Add(AdvanceOffset(Expression.Constant(1)));
        for (var m = 0; m < maskByteCount; m++)
        {
            body.Add(Expression.Call(writeByteMethod, SliceSpan(1), Expression.Convert(maskVars[m], typeof(byte))));
            body.Add(AdvanceOffset(Expression.Constant(1)));
        }

        // Pass 4: write each field's payload, in original declaration order.
        for (var i = 0; i < properties.Length; i++)
        {
            var kind = kinds[i];

            if (presenceBitIndex[i] < 0)
            {
                // Non-nullable value fields carry no presence bit at all: write only the payload.
                var (writeMethod, _) = CodecMethods(kind);
                var fixedSize = FixedSizeOf(kind);
                var propExpr = Expression.Property(valueParam, properties[i]);
                body.Add(Expression.Call(writeMethod, SliceSpan(fixedSize), propExpr));
                body.Add(AdvanceOffset(Expression.Constant(fixedSize)));
            }
            else if (IsNullableValueKind(kind))
            {
                var (writeMethod, _) = CodecMethods(kind);
                var fixedSize = FixedSizeOf(kind);
                var writeBlock = Expression.Block(
                    Expression.Call(writeMethod, SliceSpan(fixedSize), Expression.Property(fieldValVars[i]!, "Value")),
                    AdvanceOffset(Expression.Constant(fixedSize)));
                body.Add(Expression.IfThen(presentVars[i]!, writeBlock));
            }
            else if (kind == FieldKind.String)
            {
                var writeStringMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.WriteString))!;
                var writeBlock = Expression.Block(
                    Expression.Call(writeInt32Method, SliceSpan(FieldCodecs.LengthPrefixSize), varLenVars[i]!),
                    AdvanceOffset(Expression.Constant(FieldCodecs.LengthPrefixSize)),
                    Expression.Call(writeStringMethod, SliceSpanVar(varLenVars[i]!), fieldValVars[i]!),
                    AdvanceOffset(varLenVars[i]!));
                body.Add(Expression.IfThen(presentVars[i]!, writeBlock));
            }
            else // ByteArray
            {
                var writeBytesMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.WriteBytes))!;
                var writeBlock = Expression.Block(
                    Expression.Call(writeInt32Method, SliceSpan(FieldCodecs.LengthPrefixSize), varLenVars[i]!),
                    AdvanceOffset(Expression.Constant(FieldCodecs.LengthPrefixSize)),
                    Expression.Call(writeBytesMethod, SliceSpanVar(varLenVars[i]!), fieldValVars[i]!),
                    AdvanceOffset(varLenVars[i]!));
                body.Add(Expression.IfThen(presentVars[i]!, writeBlock));
            }
        }

        body.Add(Expression.Call(writerParam, advanceMethod, totalVar));

        var allVars = new List<ParameterExpression> { totalVar, spanVar, offsetVar };
        allVars.AddRange(fieldValVars.Where(v => v is not null)!);
        allVars.AddRange(presentVars.Where(v => v is not null)!);
        allVars.AddRange(varLenVars.Where(v => v is not null)!);
        allVars.AddRange(maskVars);
        var block = Expression.Block(typeof(void), allVars, body);
        return Expression.Lambda<SerializeAction<T>>(block, valueParam, writerParam).Compile();
    }

    // ── Read ─────────────────────────────────────────────────────────────────

    private static SpanReader<T> BuildRead<T>(PropertyInfo[] properties, FieldKind[] kinds, int[] presenceBitIndex, int maskByteCount, int presenceBearingCount, ConstructorInfo ctor)
    {
        var dataParam = Expression.Parameter(typeof(ReadOnlySpan<byte>), "data");
        var instanceVar = Expression.Variable(typeof(T), "instance");
        var offsetVar = Expression.Variable(typeof(int), "offset");
        var maskByteVar = Expression.Variable(typeof(byte), "maskByte");
        var lenVar = Expression.Variable(typeof(int), "len");

        var readByteMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.ReadByte))!;
        var readInt32Method = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.ReadInt32))!;
        var validateVersionMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.ValidateVersion))!;
        var validateMaskMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.ValidateMask))!;

        Expression SliceSrc(int length) => Expression.Call(dataParam, ReadOnlySpanSlice, offsetVar, Expression.Constant(length));
        Expression SliceSrcVar(Expression lengthExpr) => Expression.Call(dataParam, ReadOnlySpanSlice, offsetVar, lengthExpr);
        Expression AdvanceOffset(Expression amount) => Expression.AddAssign(offsetVar, amount);

        var maskVars = new ParameterExpression[maskByteCount];
        for (var m = 0; m < maskByteCount; m++)
            maskVars[m] = Expression.Variable(typeof(int), $"mask_{m}");

        Expression IsPresent(int p) => Expression.NotEqual(
            Expression.And(maskVars[p / 8], Expression.Constant(1 << (p % 8))),
            Expression.Constant(0));

        var body = new List<Expression>
        {
            Expression.Assign(instanceVar, Expression.New(ctor)),
            Expression.Assign(offsetVar, Expression.Constant(0)),
            Expression.Call(validateVersionMethod, Expression.Call(readByteMethod, SliceSrc(1))),
            AdvanceOffset(Expression.Constant(1))
        };

        // Read + validate the shared presence bitmask, right after the version byte.
        for (var m = 0; m < maskByteCount; m++)
        {
            var usedBitCount = Math.Min(8, presenceBearingCount - m * 8);
            body.Add(Expression.Assign(maskByteVar, Expression.Call(readByteMethod, SliceSrc(1))));
            body.Add(AdvanceOffset(Expression.Constant(1)));
            body.Add(Expression.Call(validateMaskMethod, maskByteVar, Expression.Constant(usedBitCount)));
            body.Add(Expression.Assign(maskVars[m], Expression.Convert(maskByteVar, typeof(int))));
        }

        for (var i = 0; i < properties.Length; i++)
        {
            var kind = kinds[i];
            var propTarget = Expression.Property(instanceVar, properties[i]);

            if (IsNullableValueKind(kind))
            {
                var (_, readMethod) = CodecMethods(kind);
                var fixedSize = FixedSizeOf(kind);
                var ifTrue = Expression.Block(
                    Expression.Assign(propTarget, Expression.Convert(Expression.Call(readMethod, SliceSrc(fixedSize)), properties[i].PropertyType)),
                    AdvanceOffset(Expression.Constant(fixedSize)));
                var ifFalse = Expression.Assign(propTarget, Expression.Constant(null, properties[i].PropertyType));
                body.Add(Expression.IfThenElse(IsPresent(presenceBitIndex[i]), ifTrue, ifFalse));
            }
            else if (kind == FieldKind.String)
            {
                var readStringMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.ReadString))!;
                var ifTrue = Expression.Block(
                    Expression.Assign(lenVar, Expression.Call(readInt32Method, SliceSrc(FieldCodecs.LengthPrefixSize))),
                    AdvanceOffset(Expression.Constant(FieldCodecs.LengthPrefixSize)),
                    Expression.Assign(propTarget, Expression.Call(readStringMethod, SliceSrcVar(lenVar))),
                    AdvanceOffset(lenVar));
                var ifFalse = Expression.Assign(propTarget, Expression.Constant(null, typeof(string)));
                body.Add(Expression.IfThenElse(IsPresent(presenceBitIndex[i]), ifTrue, ifFalse));
            }
            else if (kind == FieldKind.ByteArray)
            {
                var readBytesMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.ReadBytes))!;
                var ifTrue = Expression.Block(
                    Expression.Assign(lenVar, Expression.Call(readInt32Method, SliceSrc(FieldCodecs.LengthPrefixSize))),
                    AdvanceOffset(Expression.Constant(FieldCodecs.LengthPrefixSize)),
                    Expression.Assign(propTarget, Expression.Call(readBytesMethod, SliceSrcVar(lenVar))),
                    AdvanceOffset(lenVar));
                var ifFalse = Expression.Assign(propTarget, Expression.Constant(null, typeof(byte[])));
                body.Add(Expression.IfThenElse(IsPresent(presenceBitIndex[i]), ifTrue, ifFalse));
            }
            else
            {
                // Non-nullable value fields carry no presence bit at all: read the payload directly.
                var (_, readMethod) = CodecMethods(kind);
                var fixedSize = FixedSizeOf(kind);
                body.Add(Expression.Assign(propTarget, Expression.Call(readMethod, SliceSrc(fixedSize))));
                body.Add(AdvanceOffset(Expression.Constant(fixedSize)));
            }
        }

        body.Add(instanceVar);
        var allVars = new List<ParameterExpression> { instanceVar, offsetVar, maskByteVar, lenVar };
        allVars.AddRange(maskVars);
        var block = Expression.Block(typeof(T), allVars, body);
        return Expression.Lambda<SpanReader<T>>(block, dataParam).Compile();
    }

    // ── Fixed-only fast path ─────────────────────────────────────────────────
    // Used only when presenceBearingCount == 0 (every field is a fixed-size, non-nullable value kind —
    // no Nullable<T>/string/byte[] at all). Every field's offset is known at build time, so there is no
    // mutable offset variable and no mask: offsets are baked in as Expression.Constant, and Measure is
    // simply a constant. Deliberately separate from BuildMeasure/BuildWrite/BuildRead above rather than
    // sharing code with them, per this phase's brief.

    private static Func<T, int> BuildMeasureFixedOnly<T>(FieldKind[] kinds)
    {
        var total = 1 + kinds.Sum(FixedSizeOf); // format version byte + every field's fixed width
        var valueParam = Expression.Parameter(typeof(T), "value");
        return Expression.Lambda<Func<T, int>>(Expression.Constant(total), valueParam).Compile();
    }

    private static SerializeAction<T> BuildSerializeIntoFixedOnly<T>(PropertyInfo[] properties, FieldKind[] kinds)
    {
        var total = 1 + kinds.Sum(FixedSizeOf); // build-time-known constant, same formula as BuildMeasureFixedOnly

        var valueParam = Expression.Parameter(typeof(T), "value");
        var writerParam = Expression.Parameter(typeof(IBufferWriter<byte>), "writer");
        var spanVar = Expression.Variable(typeof(Span<byte>), "span");

        var getSpanMethod = typeof(IBufferWriter<byte>).GetMethod(nameof(IBufferWriter<byte>.GetSpan))!;
        var advanceMethod = typeof(IBufferWriter<byte>).GetMethod(nameof(IBufferWriter<byte>.Advance))!;
        var writeByteMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.WriteByte))!;

        // The total size is already known at build time, so it's baked directly into GetSpan/Advance —
        // no runtime call to Measure at all, unlike the generic path (which must compute size per call).
        var body = new List<Expression>
        {
            Expression.Assign(spanVar, Expression.Call(writerParam, getSpanMethod, Expression.Constant(total))),
            Expression.Call(writeByteMethod,
                Expression.Call(spanVar, SpanSlice, Expression.Constant(0), Expression.Constant(1)),
                Expression.Constant(FieldCodecs.FormatVersion))
        };

        var offset = 1;
        for (var i = 0; i < properties.Length; i++)
        {
            var kind = kinds[i];
            var (writeMethod, _) = CodecMethods(kind);
            var size = FixedSizeOf(kind);
            var propExpr = Expression.Property(valueParam, properties[i]);
            var destSlice = Expression.Call(spanVar, SpanSlice, Expression.Constant(offset), Expression.Constant(size));
            body.Add(Expression.Call(writeMethod, destSlice, propExpr));
            offset += size;
        }

        body.Add(Expression.Call(writerParam, advanceMethod, Expression.Constant(total)));

        var block = Expression.Block(typeof(void), [spanVar], body);
        return Expression.Lambda<SerializeAction<T>>(block, valueParam, writerParam).Compile();
    }

    private static SpanReader<T> BuildReadFixedOnly<T>(PropertyInfo[] properties, FieldKind[] kinds, ConstructorInfo ctor)
    {
        var dataParam = Expression.Parameter(typeof(ReadOnlySpan<byte>), "data");
        var instanceVar = Expression.Variable(typeof(T), "instance");

        var readByteMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.ReadByte))!;
        var validateVersionMethod = typeof(FieldCodecs).GetMethod(nameof(FieldCodecs.ValidateVersion))!;

        var body = new List<Expression>
        {
            Expression.Assign(instanceVar, Expression.New(ctor)),
            Expression.Call(validateVersionMethod,
                Expression.Call(readByteMethod,
                    Expression.Call(dataParam, ReadOnlySpanSlice, Expression.Constant(0), Expression.Constant(1))))
        };

        var offset = 1;
        for (var i = 0; i < properties.Length; i++)
        {
            var kind = kinds[i];
            var (_, readMethod) = CodecMethods(kind);
            var size = FixedSizeOf(kind);
            var propTarget = Expression.Property(instanceVar, properties[i]);
            var srcSlice = Expression.Call(dataParam, ReadOnlySpanSlice, Expression.Constant(offset), Expression.Constant(size));
            body.Add(Expression.Assign(propTarget, Expression.Call(readMethod, srcSlice)));
            offset += size;
        }

        body.Add(instanceVar);
        var block = Expression.Block(typeof(T), [instanceVar], body);
        return Expression.Lambda<SpanReader<T>>(block, dataParam).Compile();
    }
}
