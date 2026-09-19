# Synthetic projection robustness v1

`result.json` contains six designed target variants and three policies (18
observations). Seventeen executions complete and replay locally; the causal
prototype rejects the conflicting batch before consuming any target choice.

The model is an intentionally faulty two-replica toy, not Raft. The occurrence
policy uses the real v1 projector. In the reordered case it reports exact
coverage but moves faults to the wrong operations and loses the source failure.

See [the study](../../../PROJECTION_STUDY.md) for inputs, policies, raw-outcome
interpretation, limitations, and reproduction. `--verify` regenerates exact
bytes. The source and checksum are committed together; this artifact makes no
producer-build, timing, independent-verifier, or production-effectiveness claim.
