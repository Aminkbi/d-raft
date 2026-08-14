# Counterexample minimization

`draft minimize` accepts a replayable violation artifact and preserves one
explicit checker fingerprint throughout reduction. It first verifies the input
with exact tape replay.

## Reduction phases

1. Delta-debug ordered external scenario actions by removing chunks and
   rerunning from a clean cluster.
2. Delta-debug semantic guidance entries. A `GuidedDecider` reuses a selection
   only when choice ID, kind, domain digest, and context digest all match;
   unmatched choices use a documented seeded fallback.
3. Apply domain-aware shrinking: ranges move toward their minimum through
   bounded intermediate values, and network choices prefer a legal drop before
   the first declared option.

Every accepted candidate must reproduce the target fingerprint. Removing an
ancestor naturally makes dependent choices unmatched; they are not forced onto
a different message or timer.

## Output and metrics

The output is a fresh, exact `d-raft.run/v1` artifact containing the full tape
actually consumed by the minimized execution. The command also reports actions
removed, guidance entries proven unnecessary, selections simplified, reruns,
and whether the minimization budget was exhausted.

“Guidance removed” is a causal-core metric, not the number of entries omitted
from the final exact tape: deterministic replay still records every choice it
consumed. A future compact counterexample wrapper may encode sparse guidance
plus its fallback policy, but it will not change the already published run-v1
meaning.

## Limitations

Reduction is greedy and fingerprint-specific; it does not prove a globally
minimal counterexample. Non-monotonic failures can defeat delta debugging and
numeric simplification. Current action reduction does not remap membership or
produce asymmetric partition matrices. Comparative evaluation will measure it
against flat ddmin and a DEMi-inspired baseline.
