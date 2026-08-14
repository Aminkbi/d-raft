# Security policy

## Scope

d-raft is research and testing infrastructure. Its pseudo-random generator is
not cryptographically secure, and the simulator is not a production network,
sandbox, or trust boundary. Do not use it to protect secrets or execute
untrusted callbacks.

Security issues in the repository itself—especially unsafe trace disclosure,
dependency or workflow compromise, and unexpected file or command execution—
should be reported privately through GitHub's **Report a vulnerability** flow:

<https://github.com/aminkbi/d-raft/security/advisories/new>

Please include the affected commit or version, impact, reproduction steps, and
any suggested mitigation. Avoid opening a public issue until a coordinated
fix or disclosure plan is agreed.
