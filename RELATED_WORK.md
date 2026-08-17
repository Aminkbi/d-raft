# Related work and positioning

**Author:** Mohammadamin Khanbabaei (`aminkbi`)  
**Review date:** 2026-08-17

This matrix positions d-raft against public papers, documentation, and pinned
software snapshots. It is not a superiority ranking. The systems operate at
different layers, and “not a documented focus” does not prove that a capability
is absent from unpublished or later work.

## Comparison matrix

| Work | Controlled execution or search | Failure representation and reduction | Checking or evidence | Portability boundary | Relationship to d-raft |
| --- | --- | --- | --- | --- | --- |
| Raft paper and formal specification [1] | Defines the protocol and safety argument; formal models support exhaustive reasoning within their abstraction | Counterexample traces arise from the model rather than a production execution artifact pipeline | Formal invariants and proof-oriented specification | Protocol specification, not an adapter-neutral runtime replay format | Foundation for d-raft's model and invariants; d-raft does not claim to replace proof or model checking |
| etcd/raft v3.7.0 and TLA+ trace validation [2,3] | Deterministic `RawNode` core; implementation traces can be checked against a formal model | Local traces and inputs, not a documented semantic-choice minimization/portable-plan format | Mature production core plus TLA+ trace validation | Exact behavior is implementation-local | d-raft uses this exact version as its independent production-core adapter and adds negotiated, explicitly partial cross-adapter projection |
| FoundationDB Simulation [4] | Single-process deterministic whole-system simulation with randomized failures at very large scale | Seeds and deterministic reruns support diagnosis; small portable Raft counterexamples are not its documented target | Application/system assertions across simulated machines, networks, and storage | Deeply integrated with FoundationDB and Flow | Strong prior art for deterministic simulation; d-raft's narrower target is versioned semantic failure artifacts and cross-adapter replay evidence |
| SAMC [5] | Semantic-aware model checking prioritizes and reduces the distributed-system state space | Uses system semantics to accelerate deep-bug discovery | Checks target system behaviors during systematic exploration | System-specific semantic policies | Direct prior art for semantic systematic exploration; d-raft does not claim semantic guidance itself as novel |
| DEMi [6] | Re-executes faulty distributed executions while minimizing them | Causality-aware minimization is a central contribution | Preserves the failure while reducing execution events | Targets executions of one instrumented distributed system | Closest reduction baseline; d-raft specializes reduction to versioned semantic choices and stable violation fingerprints |
| Oddity [7] | Human-guided interactive scheduling for distributed systems, including a Raft example | Interactive exploration rather than automated minimal semantic tapes | Visual state/message inspection | Systems instrumented for the Oddity interface | Complementary diagnosis interface; d-raft emphasizes machine-verifiable artifacts and exact replay |
| Microsoft Coyote [8] | Systematic controlled testing of concurrent and distributed code | Reproducible schedules and bug traces | Runtime safety/liveness monitors and controlled scheduling | Applications integrated with the Coyote runtime/tooling | Broad systematic-testing prior art; d-raft is a Raft-focused virtual-time and artifact pipeline |
| MadRaft / MadSim [9] | Deterministic simulation for an educational Raft lab in Rust | Reproducible simulated failures support lab testing | Lab assertions and workload outcomes | MadSim-based Raft lab derived from educational exercises | Close deterministic-Raft testbed precedent; d-raft targets a versioned semantic artifact, witness, minimization, and negotiated adapter boundary rather than a Raft implementation lab |
| Turmoil [10] | Deterministic network simulation for distributed applications | Reproducible test scenarios | Test assertions over simulated network behavior | Rust applications using Turmoil | General deterministic-network-testing prior art, not a portable Raft counterexample format |
| TigerBeetle VOPR [11] | Deterministic simulation runs TigerBeetle's production code with seed-and-commit identity, accelerated time, and network/storage faults | Exact reproduction from seed and commit supports debugging | Safety and liveness assertions over the simulated system | Deeply integrated with TigerBeetle's production implementation | Especially close prior art for deterministic production-code testing; d-raft's narrower experiment concerns explicit semantic tapes, minimized evidence, and negotiated Raft-adapter projection |
| Antithesis deterministic testing and published Raft findings [12] | Commercial deterministic simulation and exploration of full systems | Reproducible failures are part of the product workflow; internal formats are not fully public | System properties and autonomous fault exploration | Deployed binaries inside the Antithesis environment | Important evidence that deterministic testing finds real Raft bugs; public material does not establish d-raft's exact artifact/projection design as redundant or superior |
| **d-raft** | Seeded random runs, exact semantic tapes, bounded prefix DFS, and collision-safe exact frontier caching | Scenario ddmin plus fingerprint-preserving semantic deletion and domain shrinking | Package-separated Raft witnesses, strict schemas, outcome digests, mutant evidence, and application commitments | Exact local replay; capability-negotiated cross-adapter plans with explicit exact/partial/failed projection | Research contribution is the combination and evidence contract, not any individual deterministic-testing technique |

## Narrow contribution claim

d-raft does **not** claim novelty for deterministic simulation, pure `Step`
interfaces, seeded replay, systematic scheduling, state caching, invariant
checking, delta debugging, TLA+ validation, or production-Raft adapters in
isolation.

The narrower research proposition is that a failure artifact becomes more
useful when it combines:

1. a versioned semantic-choice tape that rejects identity/domain drift;
2. a structured, stable invariant witness and independently recomputable
   outcome commitments;
3. fingerprint-preserving scenario and choice minimization;
4. source-tape provenance and capability negotiation before projection; and
5. explicit exact, partial, or failed cross-implementation projection evidence.

The current repository demonstrates that contract on one reference model, one
production-core adapter, a six-mutant known-fault corpus, and two immutable
cross-adapter cases. It does not yet establish that the combination is superior
for real-defect yield or human diagnosis time.

## Baseline gaps

The v1 bounded study does not run head-to-head implementations of SAMC, DEMi,
Coyote, MadSim, or Antithesis. It also does not conduct the user study needed
for diagnosis-time claims. Those omissions are recorded as limitations rather
than converted into novelty claims. A future effectiveness study should use
real or preregistered seeded defects, equal transition or wall-time ceilings,
time-to-first-failure, distinct fingerprints, and multiple production adapters.

## Pinned references

1. Diego Ongaro and John Ousterhout. “In Search of an Understandable Consensus
   Algorithm.” *USENIX ATC 2014*, pp. 305–319.
   <https://www.usenix.org/conference/atc14/technical-sessions/presentation/ongaro>
2. etcd-io/raft, tag `v3.7.0`, commit
   [`b867cf13f6bc0dae21204302df97bc2355c3af55`](https://github.com/etcd-io/raft/tree/b867cf13f6bc0dae21204302df97bc2355c3af55),
   the version used by d-raft's adapter.
3. etcd/raft TLA+ trace-validation material at the same pinned revision:
   <https://github.com/etcd-io/raft/tree/b867cf13f6bc0dae21204302df97bc2355c3af55/tla>
4. FoundationDB, “Simulation and Testing,” documentation version 7.3.79,
   <https://apple.github.io/foundationdb/testing.html>; source snapshot consulted:
   [`4c3f9b2870a657a28b6123d65c6fd134838b6947`](https://github.com/apple/foundationdb/tree/4c3f9b2870a657a28b6123d65c6fd134838b6947).
5. Tanakorn Leesatapornwongsa, Mingzhe Hao, Pallavi Joshi, Jeffrey F. Lukman,
   and Haryadi S. Gunawi. “SAMC: Semantic-Aware Model Checking for Fast
   Discovery of Deep Bugs in Cloud Systems.” *OSDI 2014*, pp. 399–414.
   [DBLP record](https://dblp.org/rec/conf/osdi/LeesatapornwongsaHJLG14),
   [USENIX paper page](https://www.usenix.org/conference/osdi14/technical-sessions/presentation/leesatapornwongsa).
6. Colin Scott, Aurojit Panda, Vjekoslav Brajkovic, George C. Necula, Arvind
   Krishnamurthy, and Scott Shenker. “Minimizing Faulty Executions of
   Distributed Systems.” *NSDI 2016*, pp. 291–309.
   [DBLP record](https://dblp.org/rec/conf/nsdi/ScottPBNKS16),
   [USENIX paper page](https://www.usenix.org/conference/nsdi16/technical-sessions/presentation/scott).
7. UW PLSE Oddity, pinned commit
   [`81c1a6af203a0d8e71138a27655e3c4003357127`](https://github.com/uwplse/oddity/tree/81c1a6af203a0d8e71138a27655e3c4003357127).
8. Microsoft Coyote, pinned commit
   [`f2c135d201341ee5eff3d82cac62bdb85b25139f`](https://github.com/microsoft/coyote/tree/f2c135d201341ee5eff3d82cac62bdb85b25139f).
9. MadRaft / MadSim, pinned commit
   [`8ec4565ecd3844f54f52865f2d7b3da4044f657f`](https://github.com/madsim-rs/madraft/tree/8ec4565ecd3844f54f52865f2d7b3da4044f657f).
10. Tokio Turmoil, pinned commit
    [`684acc1a8eea3a9cf2c6959dc47b69dba981cac1`](https://github.com/tokio-rs/turmoil/tree/684acc1a8eea3a9cf2c6959dc47b69dba981cac1).
11. TigerBeetle, “VOPR,” pinned commit
    [`97c7a8ef385270ebe0e1b75959d3d21d134629df`](https://github.com/tigerbeetle/tigerbeetle/blob/97c7a8ef385270ebe0e1b75959d3d21d134629df/docs/internals/vopr.md).
12. Antithesis. “Finding Bugs in Raft Implementations” (2026).
    <https://antithesis.com/blog/2026/finding-bugs-in-raft-implementations/>

Software-head revisions in references 4 and 7–11 were resolved on 2026-08-17;
they identify the exact public snapshots consulted and are not dependency pins.
