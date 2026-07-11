# Phase 1 baseline — BinarySerializer<T> (pre-optimization)

Captured from the current wire format: 1 format-version byte, then per field (in `MetadataToken`
order) a 1-byte presence flag for **every** field — including non-nullable primitives, which always
write `1` — followed by the payload. This is the baseline later phases (presence-byte removal,
bitmask presence, fixed-only fast path, single-pass string measurement) will be diffed against.

## Environment
- BenchmarkDotNet v0.14.0, Ubuntu 24.04.3 LTS (Noble Numbat)
- AMD Ryzen 7 PRO 8840HS w/ Radeon 780M Graphics, 1 CPU, 16 logical / 8 physical cores
- .NET SDK 10.0.109, .NET 10.0.9 (10.0.926.27113), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI
- Configuration: Release, `dotnet run -c Release --project tests/SharpDB.V2.Benchmarks -- --filter *Serialization*`

## Steady-state (default job: pilot + warmup + 15 iterations, [MemoryDiagnoser])

### FixedOnlyRow (8 non-nullable primitives, no strings/byte[])
| Method      | Mean       | Error     | StdDev    | Gen0   | Allocated |
|------------ |-----------:|----------:|----------:|-------:|----------:|
| Measure     |  0.2299 ns | 0.0132 ns | 0.0117 ns |      - |         - |
| Serialize   | 15.1031 ns | 0.1945 ns | 0.1819 ns |      - |         - |
| Deserialize | 27.5909 ns | 0.2760 ns | 0.2581 ns | 0.0115 |      96 B |
| RoundTrip   | 46.4335 ns | 0.9481 ns | 1.0145 ns | 0.0114 |      96 B |

Computed payload size: **80 bytes** (1 version + 8 × (1 presence + fixed width): 5+9+9+2+17+17+9+11).

### NullableHeavyRow (same 8 fields as `Nullable<T>`, 4 present / 4 absent in the sample)
| Method      | Mean      | Error     | StdDev    | Gen0   | Allocated |
|------------ |----------:|----------:|----------:|-------:|----------:|
| Measure     |  2.145 ns | 0.0113 ns | 0.0088 ns |      - |         - |
| Serialize   | 17.295 ns | 0.2031 ns | 0.1900 ns |      - |         - |
| Deserialize | 30.856 ns | 0.6198 ns | 0.8484 ns | 0.0181 |     152 B |
| RoundTrip   | 52.551 ns | 1.0550 ns | 1.5131 ns | 0.0181 |     152 B |

Computed payload size: **45 bytes** (1 version + 4 present fields × (1 + width) + 4 absent fields × 1).

### StringHeavyRow (6 strings: short ASCII, 200-char ASCII, 42-byte-UTF8 unicode, 5000-char ASCII, empty, null)
| Method      | Mean        | Error     | StdDev     | Gen0   | Gen1   | Allocated |
|------------ |------------:|----------:|-----------:|-------:|-------:|----------:|
| Measure     |    97.50 ns |  1.386 ns |   1.297 ns |      - |      - |         - |
| Serialize   |   316.10 ns |  2.238 ns |   1.984 ns |      - |      - |         - |
| Deserialize |   571.76 ns | 11.351 ns |  27.414 ns | 1.2684 | 0.0486 |   10624 B |
| RoundTrip   | 1,026.05 ns | 38.524 ns | 113.590 ns | 1.2684 | 0.0477 |   10624 B |

Computed payload size: **5274 bytes** (dominated by the 5000-char field: 5005 bytes alone).
Note: `Measure` calls `FieldCodecs.MeasureString` once per string field; `Serialize` calls it **again**
inside `Write` — this double UTF-8 byte-count pass is the target of Phase 5.

### ByteArrayHeavyRow (4 arrays: empty, 8B, 1KB, 16KB, + 1 null field)
| Method      | Mean         | Error      | StdDev      | Median     | Gen0   | Allocated |
|------------ |-------------:|-----------:|------------:|-----------:|-------:|----------:|
| Measure     |     1.969 ns |  0.0493 ns |   0.0462 ns |   1.964 ns |      - |         - |
| Serialize   |   221.704 ns |  2.5792 ns |   2.1537 ns | 221.836 ns |      - |         - |
| Deserialize |   770.903 ns | 38.9879 ns | 113.1109 ns | 720.492 ns | 2.0962 |   17544 B |
| RoundTrip   | 1,012.338 ns | 38.3178 ns | 111.7750 ns | 980.582 ns | 2.0962 |   17544 B |

Computed payload size: **17438 bytes** (dominated by the 16KB field: 16389 bytes alone).
Note: `FieldCodecs.ReadBytes` allocates a fresh `byte[]` copy per field (`src.ToArray()`) — this is
the Phase 6 zero-copy/lazy-access target; `Deserialize`'s allocated bytes here are almost entirely
these 3 non-empty array copies (8 + 1024 + 16384 ≈ 17416 B raw, plus array/object overhead).

## Cold start (`RunStrategy.ColdStart`, launchCount=5, warmupCount=0, iterationCount=1 — one fresh process per launch)
Isolates `CompiledSerializerFactory.GetOrBuild<T>()`'s one-time reflection scan + `Expression.Compile()`
cost for a `T` never seen before in the process, separate from the steady-state numbers above.

| Method         | Mean     | Error    | StdDev   |
|--------------- |---------:|---------:|---------:|
| FixedOnly      | 15.35 ms | 1.353 ms | 0.351 ms |
| NullableHeavy  | 18.68 ms | 7.804 ms | 2.027 ms |
| StringHeavy    | 19.43 ms | 4.068 ms | 1.057 ms |
| ByteArrayHeavy | 16.47 ms | 2.079 ms | 0.540 ms |

Cold start is ~15–19 ms per distinct `T` (dominated by JIT/process startup, not the reflection+compile
work itself, given the high variance across launches) — a one-time cost per closed generic type, not
a per-call cost. Steady-state calls after the first are 3–5 orders of magnitude cheaper (tens to low
hundreds of nanoseconds).

## Summary observations
- Fixed-width, non-nullable rows are already very cheap (~15 ns to serialize, no allocation).
- Every `Deserialize` allocates at least the constructed object; string/byte[]-heavy rows additionally
  allocate one object per variable-length field (unavoidable under the current `T Deserialize(...)`
  materializing API — see Phase 6 for an optional non-materializing alternative).
- `Serialize` never allocates for any row shape tested (the reusable `FixedBufferWriter` + one `Measure`
  pass upfront avoids buffer growth/copy).
- The presence byte on non-nullable fields (Phase 2 target) costs 1 byte per field unconditionally —
  on `FixedOnlyRow` that is 8 of the row's 80 bytes (10%) for information that's always `1`.
