# Transaction Dependency Engine (TDE)
is an embedded Rust library
for executing optimistic, append-only transactions over
distributed datastores like Riak.

Transactions form a dependency DAG and are validated against
their read-set before commit to ensure conflict-free execution.
