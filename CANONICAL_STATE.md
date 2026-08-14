# Canonical frontier state and bounded caching

Status: implemented for the built-in reference experiment runner; experimental
while the evaluation corpus is expanded.

Bounded semantic exploration reruns every decision prefix from a clean initial
cluster. Its optional cache avoids exploring a suffix again when
two prefixes reach exactly the same semantic frontier. Clean reruns remain the
execution model: the cache recognizes redundant prefixes; it does not restore
scheduler closures or resume a serialized Go heap.

State caching is established model-checking practice. This milestone is an
engineering application of that prior art to d-raft's semantic decision-tape
explorer, not a claim of a novel reduction algorithm.

## Frontier boundary

A cacheable frontier combines a stable pre-event state with the exact choices
already consumed inside the active event and the next open choice:

1. the preceding event and all of its protocol, persistence, network, timer,
   and checker effects have completed;
2. the pending event is represented by a typed tag rather than function
   identity, or the executor is in its explicit bootstrap phase;
3. successful intra-event selections reconstruct the deterministic path from
   that boundary to the open choice; and
4. the open choice is represented by its ID, kind, domain, and context.

This avoids treating a post-error heap snapshot as state. An open choice may be
discovered while a callback is in progress. The captured pre-event state is
compared for equality; a clean prefix rerun, rather than state restoration,
reconstructs the callback path using the intra-event selections. Go stack
frames and closures are never serialized.

The controlled `experiment.ExecuteWithFrontier` runner exposes this tuple. A
generic `explore.Runner` closure may contain mutable state that the repository
cannot observe. Generic runners remain uncached unless they explicitly provide
a complete, versioned frontier encoding under the same contract.

## Markov-completeness contract

Canonical bytes are Markov-complete only when equality means that, for every
permitted suffix within the configured bounds, both frontiers have identical:

- next choice identities and domains;
- state transitions and checker evolution; and
- terminal outcomes and violation fingerprints.

This claim is scoped to the built-in reference adapter, a named canonical-state
schema version, and a fixed exploration configuration. It is not a universal
claim about arbitrary Raft implementations or callbacks. Adding behaviorally
relevant adapter state requires a schema change or must disable caching until
that state is encoded.

A frontier encoding needs, at minimum:

- virtual time;
- canonically ordered volatile protocol status for every process;
- durable state, applied entries, application state, and installed snapshots;
- process availability and incarnation;
- active timers, including semantic kind, deadline or remaining delay,
  generation, and owning incarnation;
- in-flight messages, including semantic identity, payload, endpoints,
  delivery time and order, and relevant link or lifecycle state;
- topology, partitions, endpoint state, and link configuration;
- pending persistence operations, write tokens, completion times,
  acknowledgement state, and crash-after-persist arming;
- queued protocol inputs and barriers;
- remaining external actions and their stable order when timestamps tie;
- complete checker history needed by future observations, including leadership,
  voting, monotonic-index, commit, apply, snapshot, and deduplication witnesses;
- the staged next transition and open choice identity, kind, domain, and
  semantic context;
- exploration continuation data, including consumed prefix depth, remaining
  depth allowance, and any seeded-suffix state that can affect future choices;
  and
- scenario, adapter, decision, checker, message-codec, configuration, and
  canonical-state schema identities, unless the cache is provably confined to
  one fixed invocation.

Pointer identity, map iteration order, and Go function identity are excluded.
The first schema conservatively retains scheduler event and router packet
identifiers because they participate in cancellation, exhaustion, observation,
and insertion order. Future schema versions may normalize them only after an
equivalence proof. Stable same-time ordering is always encoded.

## Canonical encoding and equality

The state encoder uses versioned canonical JSON, while the cache identity uses
length-prefixed binary fields. It defines ordering for every collection,
preserves integer widths without lossy generic JSON conversion, and
distinguishes absent from empty values where behavior differs. Typed producers
own event payload schemas; the scheduler itself validates kind and JSON syntax.

SHA-256 is an index, not the equality relation:

```text
canonical = encode(frontier)
digest = SHA256(canonical)
bucket = cache[digest]

prune only when an entry in bucket has bytes exactly equal to canonical
```

Each digest bucket retains the full canonical bytes. Different bytes in the
same bucket are a collision, are counted, and are never merged. Hash-only
pruning is probabilistic and must not be described as sound.

The cache key or coverage record must also preserve bounded-search semantics.
In particular, reaching an equal protocol state with a different remaining
decision depth is not automatically equivalent for this search. An
implementation may include the relevant continuation bounds in canonical
bytes or record how much suffix depth has already been explored, but it must
never prune a frontier whose permitted suffixes are broader than the stored
coverage.

## Bounded cache behavior

Cache capacity requires positive entry-count and encoded-byte bounds and
enforces both. Admission is deterministic for a fixed run.
An entry that is not admitted causes additional clean reruns only; the first
implementation does not evict admitted entries.

Run count, depth, branch, sampling, time, and artifact limits retain their
existing meaning. Cache exhaustion is a performance condition, not evidence
that exploration is complete. Early-stop settings and global run budgets can
make traversal order observable, so cached and uncached results must report
their truncation and sampling conditions rather than being compared as if they
were exhaustive proofs.

`draft explore` reports lookups, exact hits, misses, digest collisions, retained
states, retained identity bytes, capacity bypasses, and state-pruned
prefixes. Evaluation should additionally compare:

- attempted prefixes, clean executions, and simulator transitions;
- cache lookups, digest-bucket hits, exact-byte hits, and digest collisions;
- retained unique frontier identities and prefixes pruned by exact equality;
- encoded bytes, encoding time and allocations, and peak cache memory;
- wall-clock time, throughput, and time to first failure;
- depth-bound, sampling, run-budget, and cache-capacity conditions; and
- distinct violation fingerprints plus the first counterexample identity and
  traversal position.

Derived values such as executions or transitions avoided should be labeled as
estimates unless measured against a matched uncached run.

## Validation gate

Caching is guarded by cache-off/cache-on tests for converging small models and
a reference-runner smoke scenario, plus deliberate outer-digest collisions and
canonical-state sensitivity tests. A broader false-merge and fully enumerated
evaluation matrix remains required; each pair uses the same scenario, adapter,
schema versions, bounds, seed, and branch order and must produce the same:

- reachable terminal outcomes and violation fingerprints;
- completion, depth-bound, sampling, and truncation interpretation; and
- deterministic first counterexample when the traversal is otherwise fully
  enumerated and not stopped by a global resource limit.

Tests should deliberately omit one field at a time from a test encoder and
construct frontiers whose futures diverge because of that field. Those
false-merge tests cover timers, incarnations, message ordering, pending writes,
remaining actions, checker history, and continuation depth. Collision tests
must place distinct canonical byte strings in one synthetic digest bucket and
verify that neither is pruned as equal.

For sampled or resource-truncated searches, cache-off/cache-on runs may consume
their budgets differently. Their reports must be compared with that caveat;
matching a single first failure is not a substitute for the fully enumerated
correctness gate.
