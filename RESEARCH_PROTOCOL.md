# Failure-preserving replay research protocol

Status: prospective protocol for the first production-defect study. The
synthetic development experiment in [PROJECTION_STUDY.md](PROJECTION_STUDY.md)
has already been run; this document is not an external preregistration and
does not present those fixtures as held-out evidence.

## Claim and unit of analysis

The primary hypothesis is that semantic counterexamples reproduce the same
defect across declared implementation changes more reliably than seed-only
replay. A second hypothesis is that semantic reduction decreases the evidence
needed to reproduce a defect at a measured computational cost.

One replay observation is a tuple of defect, source revision, target revision
or controlled transformation, workload, source seed/tape, method, and budget.
Repeated seeds for one defect are repeated measurements, not independent
defects. One reduction observation uses the same failing input and preservation
predicate for every reduction method.

The primary denominator is all preselected source-failing pairs supported by
all methods in a comparison. Also publish the full selected-case denominator,
capability exclusions, source-replay failures, mapping rejections, timeouts,
and infrastructure failures. Never silently replace difficult cases.

## Failure identity and three separate target questions

A defect record pins an issue/fix reference, vulnerable and fixed revisions,
fault assumptions, observation contract, and reviewed failure predicate.
Strict local fingerprints remain available. A cross-version predicate must
name the invariant, logical operation/role relationships, and evidence needed
to establish the same failure, rather than equating raw terms or log indexes.
An invariant ID alone is insufficient. Any normalization must retain its
original evidence and have negative controls for distinct defects sharing an
invariant. Predicate changes after seeing target outcomes are exploratory.

Report separately:

| Target | Question | Successful evidence |
| --- | --- | --- |
| Vulnerable version/transformation | Does the original defect survive replay? | Target witness satisfies the predeclared predicate |
| Fixed version | Does the corresponding scenario distinguish the fix? | Scenario executes to its boundary; target predicate is absent; operational failures do not count as repair |
| Independent implementation | Can it execute a meaningfully corresponding scenario? | Mapping eligibility, coverage, completion, and independently checked outcome; no requirement that it exhibit the source defect |

Absence of a bounded failure is not proof of correctness. Local tape replay
success and application-commitment agreement are reported separately from
failure preservation.

## Controlled changes and causal mapping

Development transformations include additional irrelevant messages, send
reordering, shifted timer deadlines, split/retried messages, and merged
batches. Confirm fault-free workload behavior before using a transformation;
describe exactly which fault-sensitive behavior it changes. A supported
upstream upgrade is a separate category, not automatically a harmless change.

For a production causal mapping, an adapter must expose stable logical command
IDs, protocol phase/dependency identity, process incarnation, and retry or
batch membership where needed. Derive identities without consulting the
target failure outcome. Define whether a directive addresses one attempt or
all attempts for an operation; switching those interpretations changes the
fault model. Include persistence acknowledgements as dependency edges when
they determine whether a send is causally eligible.

Reject ambiguous mappings before execution where detectable. In particular,
an indivisible target batch cannot satisfy conflicting per-operation drop and
deliver directives. Unsupported causal phases are exclusions, not successful
fallback. Target-local exact tapes continue to describe actual execution.

## Baselines and measurements

Replay baselines are seed-only re-execution, exact local tape replay (which may
reject drift), external fault intervals, current occurrence-based projection,
and a separately versioned causal prototype. Run all eligible methods on the
same target and source input. The toy study currently implements the final
three only; it does not establish a seed-only comparison.

For replay, report source/target eligibility, mapping rejection, directive
coverage, additional choices, causal misalignment where ground truth exists,
completion, failure preservation, local replay validation, and elapsed cost.
Distinguish exact *coverage* from semantic equivalence. Test fallback-seed
sensitivity on a predeclared seed list, retaining every result.

Reduction baselines are the unreduced artifact, flat stimulus ddmin, and
semantic action/guidance reduction; a DEMi-inspired baseline must document its
departures from DEMi. Hold input, target predicate, fallback policy, machine,
and wall-time ceiling constant. Report actions, executed decisions, total
artifact bytes, execution length, reruns, elapsed time, and budget exhaustion.
Guidance deletion does not necessarily reduce the final exact tape size.

Before production measurements, freeze case selection, capability rules,
seed list, budgets, baseline versions, transformation definitions, and failure
predicates in a committed study manifest. Use development cases for tuning
and separate held-out defects/versions for evaluation. Randomize paired method
order with a recorded order seed. Publish raw paired outcomes; quantify
uncertainty at the defect level, keeping within-defect repetitions grouped.
Treat timeouts as budget-exhausted/censored observations rather than dropping
them or substituting a successful-run mean. With very few defects, report
case-level outcomes without population-level effectiveness claims.

## Decision gates

1. **Identity gate (current work):** exhibit or rule out exact-coverage causal
   mismatches under controlled changes. Publish negative cases and successes.
   The synthetic reordered-message case establishes a mismatch.
2. **Adapter gate:** implement a causal contract for one real defect family;
   validate observation/identity extraction independently of the target
   outcome. Current etcd campaign scheduling and fixed-membership limitations
   constrain eligible defects.
3. **End-to-end gate:** discover, save, replay, reduce, verify, and distinguish
   a vulnerable/fixed upstream pair with the same predicate.
4. **Effectiveness gate:** evaluate frozen baselines on held-out pairs. Claim
   improved replay only if the paired results support it; otherwise narrow
   the supported change classes or report the negative result. Claim reduction
   benefit only when actual execution/artifact metrics support it.

The next milestone is gates 2 and 3 for one real defect. New adapters, cache
optimizations, a diagnosis user study, and general linearizability checking
are not prerequisites for the bounded first-paper hypothesis.
