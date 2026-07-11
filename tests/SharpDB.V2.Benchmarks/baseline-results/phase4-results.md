# Phase 4 results — fixed-only fast path

No format-version change (bytes are identical to what the generic path already produced for a
zero-presence-bearing-field `T` — this phase is a pure code-generation optimization, not a wire-format
change). `FixedOnlyRow` now routes through `BuildMeasureFixedOnly`/`BuildWriteFixedOnly`/`BuildReadFixedOnly`
instead of the generic Phase 2/3 methods; there is no way to force it back onto the old path (by design),
so the "before" comparison point is `phase3-results.md`'s `FixedOnlyRow` row (produced by the generic path
before this phase existed).

Same environment as Phases 1–3.

## Payload size
Unchanged: **72 bytes**, identical before and after — confirmed both by the existing
`FieldOrder_IsDeterministic_ByMetadataToken` test (now exercising the new path via `ThreeIntRow`) and by
`ManualFixedOnlyRowCodec.Size` (also 72) matching exactly.

## Steady-state latency (`FixedOnlyRow`, generic path in Phase 3 vs. fixed-only path this phase)

| Method | Phase 3 (generic path) | Phase 4 (fixed-only path) | Change |
|---|---:|---:|---:|
| Measure | 0.223 ns | 0.222 ns | unchanged (both already at the measurement noise floor) |
| Serialize | 15.48 ns | 14.98 ns | −3.2% (noise-band) |
| Deserialize | 27.56 ns | 27.85 ns | +1.1% (noise-band) |
| RoundTrip | 43.96 ns | 58.75 ns | +33.6% (see note below) |

`Measure`, `Serialize`, and `Deserialize` show no meaningful change — at 8 fields, the generic path's
`AddAssign`/offset-increment loop was already cheap enough that removing it doesn't move the needle
measurably; the benefit of this phase is structural (a genuinely constant `Measure`, no mutable offset
state) rather than a large win at this field count. It would matter more for wider fixed-only rows.

`RoundTrip`'s jump (43.96 ns → 58.75 ns) does **not** track `Serialize + Deserialize` in either phase
(Phase 3: 43.04 ns summed vs. 43.96 ns measured; Phase 4: 42.83 ns summed vs. 58.75 ns measured) — this
metric has been inconsistently noisy across every phase so far (Phase 1: 42.69 vs. 46.43; Phase 2: 40.57
vs. 48.41) and is not treated as a reliable signal at these single-digit-to-double-digit-nanosecond scales.

## Manual-codec reference (`ManualFixedOnlyRowCodec`, hand-written `BinaryPrimitives` code, no reflection or Expression trees)

| Method | Engine (fixed-only path) | Manual codec | Difference |
|---|---:|---:|---:|
| Measure | 0.222 ns | ~0.0002 ns | Manual is fully constant-folded by the JIT (a `const int` return) |
| Serialize | 14.98 ns | 17.99 ns | Engine is ~17% faster |
| Deserialize | 27.85 ns, 96 B | 20.18 ns, 96 B | Manual is ~28% faster |

Mixed result, reported as-is rather than rounded to a single conclusion: the manual codec is not
uniformly faster. A plausible explanation for `Deserialize` favoring the manual codec is that
`BinarySerializer<T>.Deserialize` calls through a cached `SpanReader<T>` **delegate** (`_plan.Read(data)`),
while the manual codec is a direct static method call the JIT can inline fully into the benchmark
method — delegate-invocation overhead is a real, structural cost of the compiled-Expression-tree design
that a source generator (Phase 7) would remove entirely by emitting a real method instead of a cached
delegate. This is not confirmed here (would need IL/disassembly inspection to be certain) — it is exactly
the kind of question Phase 7's cold-start/steady-state/AOT comparison is meant to answer with real
evidence, not asserted now. `Serialize` favoring the engine slightly complicates that theory and may
simply be run-to-run noise at these scales (no `Serialize`/`Deserialize` asymmetry of this kind was
predicted by the delegate-overhead theory alone).

## Summary
- No payload-size change (none was expected — this phase is codegen-only).
- No clear steady-state win at `FixedOnlyRow`'s 8-field width; the structural benefits (constant
  `Measure`, no runtime offset arithmetic) matter more as field count grows, which this benchmark doesn't
  probe directly.
- The manual-codec reference surfaced a real, open question about delegate-invocation overhead in the
  compiled-Expression-tree design — flagged for Phase 7's source-generator evaluation rather than acted
  on here, since it's not confirmed and this phase's scope is fixed-only codegen, not delegate dispatch.
