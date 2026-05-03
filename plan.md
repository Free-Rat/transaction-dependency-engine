# Transaction Dependency Engine — Project Plan

## Current State

### Architecture

The system implements optimistic transactional reads/writes over Riak. Key concepts:

- **Transactions** have states: Created → Proposed → Approved/Rejected
- **`commit()`** persists read_set/write_set to Riak, sets status to Proposed, registers tx id in `Variables[var]` for each key in write_set
- **`read(key)`** calls `find_dependency(key)` to find the latest Approved writer, auto-approves Proposed frontier tips, and uses `choose_tx()` to resolve conflicts
- **`choose_tx()`** deterministically picks a winner among Proposed transactions using UUID-distance, approves the winner, rejects the rest
- **`approve()`** changes status to Approved, removes parent tx ids from `Variables[var]`
- **`reject()`** changes status to Rejected, removes own tx id from `Variables[var]`

### What's Working (32 passing tests)

- Basic create and update: `Transaction::new()`, `commit()`, `approve()`, `reject()`, `from_uuid()`
- `find_dependency()` tip-of-chain algorithm: finds frontier writers, auto-approves single tips, resolves multiple tips via `choose_tx()`
- Auto-approval: `read()` auto-approves Proposed frontier transactions
- Genesis transactions: first writer on a variable gets auto-approved on first read
- Chain propagation: genesis → reader → writer → reader chain works correctly
- Conflict resolution: two competing writers resolved deterministically
- Nonexistent variable: `read()` returns `None` for variables that don't exist
- Riak integration: siblings handling, vclock-based conflict resolution, `add_variable_tx` / `remove_tx_from_variable` with retry
- `choose_tx()` is deterministic (same input UUIDs always produce same winner)

### What's Not Working (2 failing tests)

#### Bug 1: Cross-Variable Inconsistent Decisions (`test_cross_variable_concurrent_reads_inconsistent`)

**Scenario:**
- genesis writes A and B
- tx1 reads A from genesis, writes A
- tx2 reads A and B from genesis, writes A and B
- tx3 reads B from genesis, writes B

**Problem:**
`find_dependency("A")` only loads transactions from `Variables["A"]` = {genesis, tx1, tx2}. It never sees tx3. `find_dependency("B")` only loads {genesis, tx2, tx3}, never sees tx1. They make **independent** `choose_tx()` decisions about tx2 (the cross-variable connector). If A rejects tx2 and B approves tx2, the system reaches an inconsistent state.

**Test result:** Fails because `tx2` is Approved but doesn't serve both variables — A resolved to a different transaction than B.

#### Bug 2: Stale Proposed Transaction Wins Instead of Being Rejected (`test_stale_proposed_tx_enters_choose_tx_when_it_should_be_rejected`)

**Scenario:**
- genesis writes B → Approved
- tx1 reads B from genesis, writes B → auto-approved (single tip)
- tx2 reads B from tx1, writes B → Proposed (legitimate successor)
- stale_tx reads B from genesis, writes B → Proposed (same parent as tx1 — same generation)

**Problem:**
`find_dependency("B")` sees tx2 and stale_tx as two Proposed tips (tx1 is already Approved, so it's filtered from candidates; stale_tx's parent is genesis which is also not in the candidate writer set). It calls `choose_tx({tx2, stale_tx})`. stale_tx might win — even though it's in the **same generation** as the already-Approved tx1 (both read B from genesis). It should have been rejected because the slot it's competing for was already won by tx1.

**Test result:** Fails because `stale_tx` ends up Approved with `read_set[B] = genesis`, bypassing tx1 in the dependency chain.

## Issues (Prioritized)

### Issue 1: `choose_tx` uses UUID-distance instead of age

`choose_tx()` currently picks the winner using `uuid_distance(tx.id, target)` where target is a fixed UUID. This is deterministic but has no semantic meaning — an older transaction can lose to a newer one purely based on UUID proximity. This means:

- Two concurrent `find_dependency()` calls for different variables can pick different winners for the same shared transaction (Bug 1 is exacerbated by this)
- A stale transaction can win over a legitimate successor (Bug 2)

**Fix:** Replace UUID-distance with `Proposed.at` timestamp comparison — always pick the **oldest** Proposed transaction. This is deterministic regardless of concurrency and ensures that earlier transactions always have priority, which is semantically correct.

### Issue 2: Same-generation siblings not detected or rejected

When two transactions have the same `read_set[key]` parent (same generation), and one has already been Approved, the other(s) should be rejected immediately without entering a `choose_tx()` round. Currently, the Approved sibling (tx1) is filtered out of the writer set before tip detection, so stale_tx doesn't see it as competition.

**Fix:** After finding tip writers, check each tip's `read_set[key]` parent. If the parent already has an Approved child for the same key (i.e., there's a Approved writer whose `read_set[key]` equals the same parent), reject that tip — it's competing for a slot that's already been won.

### Issue 3: Rejected parents not cascading

If a Proposed tx depends on a Rejected parent (`read_set[key]` points to a Rejected tx), it should be rejected. The current algorithm filters out Rejected writers but doesn't explicitly reject their dependents — they just become "tips" by default (since their parent isn't in the writer set) and can win `choose_tx()` rounds.

**Fix:** After loading all transactions, mark any Proposed tx whose `read_set[key]` parent is Rejected as Rejected before tip detection.

### Issue 4: No cross-variable loading in `find_dependency()`

`find_dependency(key)` only looks at transactions registered in `Variables[key]`. When a tip transaction writes to multiple variables (e.g., tx2 writes both A and B), `find_dependency("A")` doesn't see tx3 (which only writes to B) and `find_dependency("B")` doesn't see tx1 (which only writes to A). This causes independent and potentially conflicting `choose_tx()` decisions about the shared transaction tx2.

**Fix:** In `find_dependency()`, after identifying tip writers, for each tip that writes to multiple variables, load all transactions from those other variables too. Then make a single `choose_tx()` decision that considers ALL affected variables. When approving a cross-variable winner, all variables it writes to should see it Approved; when rejecting, all variables should see it Rejected.

### Issue 5: Batch validation timestamp

`choose_tx()` calls `approve()` and `reject()` individually, each using `Utc::now()` at different microseconds. All status changes in a single validation round should share one timestamp to eliminate micro-drift edge cases where timestamps could create ambiguous ordering.

**Fix:** Accept a timestamp parameter in `choose_tx()` (or pass it through) so all `approve()`/`reject()` calls in the same round use the same instant.

## Planned Implementation

### Phase 1: Fix `choose_tx` to pick oldest (Issue 1)

Change `choose_tx()` to compare `Proposed.at` timestamps instead of UUID-distance. The oldest Proposed tx wins. This is the foundation — it makes everything else deterministic.

**File:** `src/transaction/transaction.rs` — `choose_tx()` method

```rust
// Before:
let (winner_idx, _) = txs.iter()
    .enumerate()
    .min_by_key(|(_, tx)| uuid_distance(tx.id, target))
    .unwrap();

// After:
let (winner_idx, _) = txs.iter()
    .enumerate()
    .min_by_key(|(_, tx)| {
        match &tx.state {
            TransactionStatus::Proposed { at } => *at,
            _ => chrono::DateTime::<Utc>::MAX_UTC,
        }
    })
    .unwrap();
```

Also update the two-tip preference in `find_dependency()`: when multiple tips exist and one is already Approved, prefer that one. When all tips are Proposed, the oldest one should win consistently across all variables.

### Phase 2: Reject stale same-generation siblings (Issue 2 + Issue 3)

In `find_dependency()`, after loading transactions and identifying writer indices, add two rejection passes before tip detection:

1. **Cascade Rejected parents:** If a tx's `read_set[key]` points to a Rejected tx, reject that tx.
2. **Same-generation check:** For each Proposed tip, check if there's an Approved writer with the same `read_set[key]` parent. If so, reject the tip — it's competing for an already-won slot.

**Steps in `find_dependency()`:**
- After loading all txs (step 3), before tip detection (step 5):
  - Reject any Proposed writer whose `read_set[key]` parent is Rejected
  - For each remaining Proposed writer, check if its `read_set[key]` parent matches the `read_set[key]` of any Approved writer for the same key. If yes, reject it.

### Phase 3: Cross-variable loading (Issue 4)

In `find_dependency()`, after identifying tip writers, check if any tip writes to multiple variables. If so, load all transactions from those other variable buckets and include them in the resolution decision.

**Steps in `find_dependency()`:**
- After step 4 (finding writer indices), check each writer's `write_set` for keys beyond the current `key`
- For each additional key, load `Variables[other_key]` and the corresponding transactions
- Include these cross-variable transactions in the tip detection and `choose_tx()` decision
- When a cross-variable winner is Approved/Rejected, it should be consistent across ALL variables it touches

### Phase 4: Batch validation timestamp (Issue 5)

Pass a single timestamp to `choose_tx()` and use it for all `approve()`/`reject()` calls within that round.

**Steps:**
- Add a `validated_at` parameter or calculate a single timestamp at the start of validation
- Modify `approve()` and `reject()` to accept an optional timestamp override
- In `choose_tx()`, propagate the timestamp to all status changes

## Test Suite

Existing tests that should continue to pass:
- `test_read_write_variable_lifecycle` — basic read/write/auto-approve chain
- `test_second_reader_sees_latest_value` — chain propagation
- `test_choose_tx_among_conflicting_writers` — conflict resolution
- `test_read_nonexistent_variable_returns_none` — edge case
- 26 unit/integration tests in `tests.rs` and `integration_tests.rs`

Tests that should pass after fixes:
- `test_cross_variable_concurrent_reads_inconsistent` — Bug 1 (currently fails)
- `test_stale_proposed_tx_enters_choose_tx_when_it_should_be_rejected` — Bug 2 (currently fails)
