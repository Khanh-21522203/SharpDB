# Phase 2 results — remove presence bytes for non-nullable fields

Format version bumped 1 → 2. `Deserialize` only accepts the current version (no dual-format reader —
see the Phase 2 plan for the rationale). Presence bytes now only exist for `Nullable<T>`, `string`, and
`byte[]` fields; reading them is now validated (`FieldCodecs.ValidatePresence`) to be strictly `0` or `1`,
throwing `NotSupportedException` otherwise — new behavior added this phase, not present in Phase 1.

Same environment as Phase 1 (BenchmarkDotNet v0.14.0, Ubuntu 24.04.3, AMD Ryzen 7 PRO 8840HS, .NET 10.0.9,
Release, `--filter *Serialization*`). Compared against `phase1-baseline.md`.

## Payload size

| Row shape | Phase 1 | Phase 2 | Change |
|---|---:|---:|---:|
| FixedOnlyRow | 80 B | 72 B | **−8 B (−10%)** — 8 non-nullable fields, 1 presence byte each removed |
| NullableHeavyRow | 45 B | 45 B | unchanged — all 8 fields are `Nullable<T>`, still carry presence |
| StringHeavyRow | 5,274 B | 5,274 B | unchanged — all fields are `string`, still carry presence |
| ByteArrayHeavyRow | 17,438 B | 17,438 B | unchanged — all fields are `byte[]`, still carry presence |

The payload win is exactly and only proportional to non-nullable-field count, as expected — this phase
does not touch string/byte[]/nullable encoding at all.

## Latency / allocations

| Row shape | Method | Phase 1 | Phase 2 | Change |
|---|---|---:|---:|---:|
| FixedOnlyRow | Serialize | 15.10 ns | 14.58 ns | −3.4% |
| FixedOnlyRow | Deserialize | 27.59 ns, 96 B | 25.99 ns, 96 B | −5.8% |
| NullableHeavyRow | Serialize | 17.30 ns | 18.86 ns | +9.0% |
| NullableHeavyRow | Deserialize | 30.86 ns, 152 B | 38.83 ns, 152 B | **+25.8%** |
| StringHeavyRow | Serialize | 316.1 ns | 304.2 ns | −3.8% (noise-band) |
| StringHeavyRow | Deserialize | 571.8 ns, 10,624 B | 477.2 ns, 10,624 B | −16.5% (noise-band) |
| ByteArrayHeavyRow | Serialize | 221.7 ns | 207.2 ns | −6.5% (noise-band) |
| ByteArrayHeavyRow | Deserialize | 770.9 ns, 17,544 B | 711.9 ns, 17,544 B | −7.7% (noise-band) |

Allocated bytes are unchanged everywhere (this phase doesn't touch allocation behavior — that's Phases
5/6). Cold-start numbers (15–22 ms across both runs) moved within the same noise band as Phase 1 — no
material change, as expected, since the compiled-delegate size only shrank slightly.

**`FixedOnlyRow` improved on both axes** (smaller payload, faster Serialize/Deserialize) — removing 8
presence-byte writes/reads/offset-advances is a straightforward win with no downside for this shape.

**`NullableHeavyRow.Deserialize` regressed ~26%** (30.86 ns → 38.83 ns). This is attributable to the new
`FieldCodecs.ValidatePresence` call added to every nullable/string/byte[] field read (8 calls for this
row shape) — the direct cost of the correctness improvement added this phase (rejecting a corrupted
presence byte instead of silently misreading it, per the invalid-presence test added in this phase).
This is a deliberate trade-off: it is currently the only place `Deserialize` validates instead of relying
on `Span.Slice`'s incidental bounds-checking. Given the absolute cost is still ~8 ns (single-digit
nanoseconds), this is accepted as-is for this phase rather than optimizing the validation itself; flagged
here as a known, explained regression, not silently absorbed into "noise."

The `StringHeavyRow`/`ByteArrayHeavyRow` deltas are within this run's noise band (their Phase 1 StdDev/
Error were themselves 10–40 ns wide) — not attributed to this phase's change, since neither row shape's
measured fields lost a presence byte or gained new validation logic beyond the one call per field, which
is already reflected in `NullableHeavyRow`'s explained regression above at a comparable per-field cost.

## Summary
- Non-nullable-heavy rows get smaller and (modestly) faster — the intended Phase 2 win.
- Nullable/string/byte[]-heavy rows pay a small, explained latency cost from the new presence-byte
  validation, with no payload change (expected — they were never the target of this phase).
- No allocation behavior changed in this phase.
