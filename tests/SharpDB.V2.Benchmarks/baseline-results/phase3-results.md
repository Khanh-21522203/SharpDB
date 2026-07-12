# Phase 3 results — replace per-field presence bytes with a bitmask

Format version bumped 2 → 3 (still no dual-format reader, same rationale as Phase 2). Presence-bearing
fields (`Nullable<T>`, `string`, `byte[]`) now share one leading bitmask (`ceil(N/8)` bytes for `N`
presence-bearing fields) instead of one presence byte per field. Mask bytes are validated
(`FieldCodecs.ValidateMask`) to have no bits set outside the range actually used by that byte — new,
additive validation this phase, verified via a 2-byte-mask round-trip test (9 nullable fields, crossing
the bit-7/bit-8 boundary) and a dedicated reserved-bit-corruption test (5 nullable fields, 1-byte mask
with 3 reserved bits).

Same environment as Phases 1–2. Compared against `phase2-results.md`.

## Payload size

| Row shape | Phase 2 | Phase 3 | Change |
|---|---:|---:|---:|
| FixedOnlyRow | 72 B | 72 B | unchanged — 0 presence-bearing fields, 0 mask bytes |
| NullableHeavyRow | 45 B | **38 B** | **−7 B (−15.6%)** — 8 presence bytes → 1 mask byte |
| StringHeavyRow | 5,274 B | **5,269 B** | **−5 B** — 6 presence bytes → 1 mask byte |
| ByteArrayHeavyRow | 17,438 B | **17,434 B** | **−4 B** — 5 presence bytes → 1 mask byte |

The saving is exactly `(presence-bearing field count) − maskByteCount` bytes per row, as designed —
larger for rows with more presence-bearing fields, since they all now share a single (or few) mask
byte(s) instead of paying 1 byte each.

## Latency / allocations

| Row shape | Method | Phase 2 | Phase 3 | Change |
|---|---|---:|---:|---:|
| FixedOnlyRow | Serialize | 14.58 ns | 15.48 ns | +6.2% (noise-band, unaffected code path) |
| FixedOnlyRow | Deserialize | 25.99 ns, 96 B | 27.56 ns, 96 B | +6.0% (noise-band, unaffected code path) |
| NullableHeavyRow | Serialize | 18.86 ns | 18.52 ns | −1.8% (flat) |
| NullableHeavyRow | Deserialize | 38.83 ns, 152 B | **32.73 ns, 152 B** | **−15.7%** |
| StringHeavyRow | Serialize | 304.2 ns | 313.4 ns | +3.0% (noise-band) |
| StringHeavyRow | Deserialize | 477.2 ns, 10,624 B | 587.7 ns, 10,624 B | +23.1% (high-variance run, see below) |
| ByteArrayHeavyRow | Serialize | 207.2 ns | 227.4 ns | +9.8% (noise-band) |
| ByteArrayHeavyRow | Deserialize | 711.9 ns, 17,544 B | 729.9 ns, 17,544 B | +2.5% (noise-band) |

**`FixedOnlyRow` has zero presence-bearing fields**, so its compiled Measure/Write/Read delegates take
no mask-related code path at all in either phase — its ±6% deltas are pure run-to-run measurement noise,
not attributable to this phase's change.

**`NullableHeavyRow.Deserialize` improved substantially (38.83 ns → 32.73 ns), clawing back most of
Phase 2's regression** (Phase 1 baseline was 30.86 ns). This is exactly the effect anticipated in the
Phase 3 plan: Phase 2 added 8 separate `ValidatePresence` calls (one per nullable field); Phase 3
replaces all 8 individual presence-byte reads+validations with a single mask-byte read + single
`ValidateMask` call, then a cheap bitwise AND per field instead of a full read+validate per field. The
remaining ~6% gap versus the Phase 1 baseline (which had no validation at all) is the residual cost of
that one `ValidateMask` call plus the bitwise tests — an accepted, explained cost of keeping the
correctness improvement.

**`StringHeavyRow.Deserialize` shows a nominal +23% (477 ns → 588 ns), but this run's error bars are
unusually wide** (StdDev 57.9 ns vs. a 587 ns mean, ~10% coefficient of variation, versus Phase 2's
tighter 22.4 ns StdDev) — consistent with a noisy run (GC/OS scheduling jitter) rather than a structural
regression, since `StringHeavyRow`'s mask handling is mechanically identical to `NullableHeavyRow`'s
(which measured faster, not slower, under the same change). Flagged here rather than silently absorbed;
worth re-measuring in Phase 4/5 to confirm it doesn't recur before treating it as real.

**`ByteArrayHeavyRow`** deltas are within its own noise band (Phase 2 Deserialize StdDev was already
32–113 ns across runs) — not attributed to this phase.

Allocated bytes are unchanged everywhere (this phase doesn't touch allocation behavior). Cold-start
numbers (17–20 ms) are within the same noise band established in Phases 1–2.

## Summary
- Nullable/string/byte[]-heavy rows all get smaller, proportional to their presence-bearing field count.
- `NullableHeavyRow.Deserialize` recovered most of the latency Phase 2's per-field validation cost added,
  confirming the mask consolidation pays for itself on the correctness-vs-speed trade-off Phase 2 introduced.
- `StringHeavyRow.Deserialize`'s apparent regression this run is flagged as likely noise (wide error bars,
  no matching structural reason), not claimed as a real cost — worth a rerun before Phase 4 to confirm.
- No allocation behavior changed in this phase.
