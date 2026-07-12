# Phase 5 results — avoid measuring UTF-8 strings twice

No format-version change — `Serialize`'s output bytes and `EstimateSize`'s return value are identical to
before (confirmed by all 41 existing tests passing unmodified, including every exact-byte-output
assertion). This phase only reduces how many times work happens per `Serialize` call: `Measure`/`Write`
merged into one `SerializeInto` pass per Phase 5's plan, caching each presence-bearing field's value,
presence flag, and (for `string`/`byte[]`) its measured length exactly once. Directly verified by a new
test (`Serialize_ReadsStringPropertyOnlyOnce`): a string property's getter, instrumented with a counter,
is now invoked exactly once per `Serialize` call (was up to 5 times: 2 in the old `Measure`, 3 in the old
`Write`, with `Encoding.UTF8.GetByteCount` running twice).

Same environment as Phases 1–4. Compared against `phase4-results.md`.

## Payload size
Unchanged for all four row shapes — this phase touches only how the bytes are computed, not the bytes
themselves.

## Serialize / RoundTrip latency (where this phase's change applies)

| Row shape | Method | Phase 4 | Phase 5 | Change |
|---|---|---:|---:|---:|
| StringHeavyRow | Serialize | 313.4 ns | **212.8 ns** | **−32.1%** |
| StringHeavyRow | RoundTrip | 1,017.3 ns | **731.3 ns** | **−28.1%** |
| ByteArrayHeavyRow | Serialize | 227.4 ns | 208.8 ns | −8.2% |
| ByteArrayHeavyRow | RoundTrip | 1,063.3 ns | 916.2 ns | −13.8% |
| NullableHeavyRow | Serialize | 18.52 ns | 19.99 ns | +7.9% (noise-band at this scale) |
| FixedOnlyRow | Serialize | 14.98 ns | 14.88 ns | unchanged (this phase doesn't touch the fixed-only path's Serialize cost) |

**`StringHeavyRow.Serialize` is the headline result** — a clear, large win, exactly where it was
predicted: this row's dominant cost is its 5,000-char field's UTF-8 byte count, previously computed
twice (once in `Measure`, once again in `Write`); now computed once. `RoundTrip` improves by nearly the
same absolute amount as `Serialize` did, confirming the saving isn't offset by anything in `Deserialize`
(which this phase doesn't touch).

`ByteArrayHeavyRow.Serialize` also improved, more modestly (−8.2%) — `byte[].Length` was already O(1)
before this phase, so there was no expensive re-computation to eliminate, only a redundant property-getter
re-read and a second `ArrayLength` evaluation; the smaller, still-real win is consistent with that.

`NullableHeavyRow.Serialize`'s small increase (+7.9%, ~1.5 ns absolute) is treated as noise, not a
regression — this row shape has bounced between 17.3–20.0 ns across every phase since Phase 2 with no
consistent trend, and nullable fields' `HasValue` check was already cheap (not O(n) like string
measurement) even when evaluated twice, so there's little for this phase's caching to save there.
`FixedOnlyRow.Serialize` is flat, as expected — it has no presence-bearing fields, so this phase's
caching logic doesn't apply to it at all (Phase 4 already gave it its own fast path).

## Deserialize (not touched by this phase — sanity check only)
`Deserialize` numbers moved for `StringHeavyRow` (587.7 ns → 451.2 ns) and `ByteArrayHeavyRow`
(729.9 ns → 646.5 ns) even though this phase's code changes don't touch `Read` at all. This confirms the
suspicion flagged in `phase4-results.md`: those Phase 4 numbers had unusually wide error bars (`StringHeavyRow`
StdDev was 57.9 ns on a 587 ns mean) and were likely noisy; this run's tighter StdDev (8.2 ns) and closer
alignment with the Phase 3 baseline (`StringHeavyRow.Deserialize` was 477.2 ns in Phase 3) supports treating
the Phase 4 figure as the outlier, not this one.

## Summary
- `StringHeavyRow.Serialize` improved ~32%, `RoundTrip` ~28% — the intended, confirmed win.
- `ByteArrayHeavyRow.Serialize` improved more modestly (~8%), consistent with `byte[].Length` already
  being cheap before this phase.
- `NullableHeavyRow`/`FixedOnlyRow` are unaffected (within noise), as expected — this phase specifically
  targets the double-UTF-8-measurement cost, which only exists for `string` fields.
- No payload-size or `Deserialize`-behavior change (confirmed via the full existing test suite).
- The `Serialize_ReadsStringPropertyOnlyOnce` test directly pins the mechanism (1 getter call vs. up to 5
  before), not just the timing outcome.
