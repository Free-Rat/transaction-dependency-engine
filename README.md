# Transaction Dependency Engine (TDE)

An embedded Rust library for executing **optimistic, append-only transactions** over eventually-consistent distributed datastores like Riak.

Transactions form a **dependency DAG** — each transaction records which variable versions it read from (its *read set*) and which variables it wrote to (its *write set*). The engine uses this DAG to automatically detect conflicts, resolve them deterministically, and ensure that readers always observe a consistent view of the data.

## The Problem

Distributed key-value stores like Riak are *eventually consistent* — they allow concurrent writes to the same key, store all conflicting versions as siblings, and rely on the application to resolve them. This creates several challenges:

- **No transactional guarantees.** A reader that reads two separate keys may see values from different points in time, violating consistency.
- **No conflict resolution.** When two clients write to the same variable concurrently, the store creates sibling versions. The application must decide which one wins.
- **No causal ordering.** Without tracking which versions a transaction observed, there's no way to know whether a write is based on stale data or the latest state.
- **Cross-variable inconsistency.** When a transaction writes to multiple variables, resolving each variable independently can lead to contradictory outcomes — one variable picks transaction A as the winner, while another variable picks transaction B, even though they conflict.

Traditional solutions (2PC, Paxos, Raft) solve consistency but require coordination and sacrifice availability under partitions. TDE takes a different approach.

## The Idea

TDE builds an **optimistic, append-only transaction DAG** on top of an eventually-consistent store:

1. **Transactions, not keys, are the unit of consistency.** Each transaction is a node in a DAG. Its outgoing edges (read set) point to the transactions whose values it observed. Its incoming edges are transactions that later read from it.

2. **The DAG is self-validating.** When a transaction reads a variable, the engine walks the DAG to find the *frontier* — the latest valid writer for that variable. If the frontier has unconcluded (Proposed) transactions, the reader resolves them: approving one and rejecting the rest.

3. **Append-only, no overwrites.** Variables store the IDs of all transactions that wrote to them. Old versions are never deleted — they're superseded when newer transactions are approved. The DAG naturally garbage-collects obsolete entries.

4. **Deterministic conflict resolution.** When multiple Proposed transactions compete for the same variable, `choose_tx()` deterministically selects a winner (oldest-first by timestamp). All transactions in the system reach the same decision given the same input, avoiding coordination.

5. **Lazy approval.** Genesis transactions (the first writer on a variable) start in the Proposed state. They're automatically approved when the first reader calls `read()` — no manual approval step is needed.

## How It Works

### Transaction Lifecycle

```
Created ──► Proposed ──► Approved
                     ╰──► Rejected
```

1. **Create:** `Transaction::new(client)` — creates a new transaction in the `Created` state.

2. **Write:** `tx.write("balance", b"1000")` — adds to the write set (in-memory only).

3. **Read:** `tx.read("balance")` — calls `find_dependency("balance")` to resolve the DAG frontier:
   - Finds all transactions registered in `Variables["balance"]`
   - Filters out Rejected transactions
   - Identifies *tip* writers (those not depended upon by other writers for this key)
   - Single tip → auto-approve if Proposed, return its value
   - Multiple tips → `choose_tx()` resolves conflict, approves winner, rejects losers

4. **Commit:** `tx.commit()` — persists read_set/write_set to Riak, sets status to `Proposed`, registers the transaction ID in `Variables[var]` for each key in the write set.

### The DAG in Practice

```
                   genesis
                  /       \
              tx1(A)       tx2(A,B)
                |         /       \
            tx3(A)     tx4(A)    tx5(B)
```

- `genesis` writes variables A and B
- `tx1` reads A from genesis, writes A
- `tx2` reads A and B from genesis, writes A and B (cross-variable)
- `tx3` reads A from tx1, writes A
- `tx4` reads A from tx2, writes A (competes with tx3)
- `tx5` reads B from tx2, writes B

When a reader calls `read("A")`, `find_dependency` walks the DAG starting from all writers registered in `Variables["A"]`, finds the frontier tips (tx3 and tx4 both depend on earlier Approved writers), and resolves the conflict.

### Variable Storage

Transactions are stored across several Riak buckets:

| Bucket | Key | Value |
|---|---|---|
| `ReadSets` | `tx_id` | `HashMap<String, Uuid>` — maps variable name → parent transaction |
| `WriteSets` | `tx_id` | `HashMap<String, Vec<u8>>` — maps variable name → value bytes |
| `Statuses` | `tx_id` | `TransactionStatus` — Created/Proposed/Approved/Rejected with timestamp |
| `Variables` | `variable_name` | `Vec<String>` — list of transaction IDs that wrote to this variable |

## Usage

```rust
use transaction_dependency_engine::riak::client::Client;
use transaction_dependency_engine::transaction::transaction::Transaction;

let client = Client::new("http://localhost:8098");

// Create a genesis transaction — the first writer on a variable
let mut genesis = Transaction::new(&client);
genesis.write("balance", b"1000".to_vec());
genesis.commit().await?;

// Read auto-approves genesis and returns its value
let mut reader = Transaction::new(&client);
let value = reader.read("balance").await?;
assert_eq!(value, Some(b"1000".to_vec()));

// Write a new version
reader.write("balance", b"800".to_vec());
reader.commit().await?;

// Next reader sees the updated value
let mut next = Transaction::new(&client);
let updated = next.read("balance").await?;
assert_eq!(updated, Some(b"800".to_vec()));
```

## Architecture

```
src/
├── lib.rs                          # Module declarations
├── riak/
│   ├── mod.rs                      # Riak module
│   ├── client.rs                   # HTTP client for Riak (get, put, get_deserialized, etc.)
│   └── object.rs                   # VClock type for Riak vector clocks
└── transaction/
    ├── mod.rs                      # Transaction module
    ├── transaction.rs              # Core: Transaction struct, find_dependency, choose_tx, commit, approve, reject
    ├── tests.rs                    # Unit tests with httptest mocks
    ├── integration_tests.rs        # Integration tests against real Riak
    └── lifecycle_tests.rs          # End-to-end lifecycle tests with verbose output
```

## Known Issues

See [plan.md](plan.md) for detailed analysis and fix strategy.

| Issue | Description | Status |
|---|---|---|
| **Cross-variable inconsistency** | `find_dependency()` only loads transactions from one variable's bucket, causing independent and potentially conflicting `choose_tx()` decisions about shared transactions | Bug test fails |
| **Stale transaction wins** | A Proposed transaction in the same generation as an already-Approved sibling can win `choose_tx()` instead of being rejected | Bug test fails |
| **Rejected parents not cascading** | A transaction whose read-set parent was Rejected can still appear as a tip and win | Not yet tested |
| **choose_tx uses UUID-distance** | The current `choose_tx()` uses UUID proximity, which is deterministic but semantically meaningless — newer transactions can win over older ones | To be fixed |

## Development Setup

The project uses [Nix Flakes](https://nixos.wiki/wiki/Flakes) for reproducible development environments. The `flake.nix` provides a shell with all required tools and automatically starts a Riak container.

### Prerequisites

- [Nix](https://nixos.org/download) with flakes enabled
- Docker (daemon must be running — the flake starts a Riak container via `docker run`)

### Entering the dev shell

```bash
nix develop
```

This provides:

| Tool | Purpose |
|---|---|
| `rustc`, `cargo` | Rust toolchain (edition 2024) |
| `docker` | Docker client for running Riak |
| `docker-compose` | Container orchestration |
| `curl` | Manual Riak API inspection |
| `git`, `bash` | Standard dev utilities |

The shell hook automatically starts a Riak container:

```bash
docker run --name=riak -d -p 8087:8087 -p 8098:8098 basho/riak-kv
```

Riak will be available at `http://localhost:8098` (HTTP) and `localhost:8087` (Protocol Buffers).

### If Riak is already running

If the `riak` container already exists, the shell hook will print an error from `docker run` (name conflict) — this is harmless. You can check status with:

```bash
docker ps --filter name=riak
```

To restart Riak:

```bash
docker rm -f riak
docker run --name=riak -d -p 8087:8087 -p 8098:8098 basho/riak-kv
```

### Running Tests

```bash
# Unit + integration tests (require Riak on localhost:8098)
cargo test --lib

# Lifecycle tests with verbose output
cargo test --lib lifecycle_tests -- --nocapture --test-threads=1

# Only unit tests (mocked, no Riak needed)
cargo test --lib tests
```

## License

MIT
