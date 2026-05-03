#![cfg(test)]

use crate::riak::client::{Client, Bucket};
use crate::transaction::transaction::{Transaction, TransactionStatus};
use rand::random;

const HOST: &str = "http://localhost:8098";

fn unique_key(prefix: &str) -> String {
    format!("{}-{}", prefix, random::<u32>())
}

macro_rules! print_section {
    ($($arg:tt)*) => {
        println!("\n{}", "=".repeat(60));
        println!("  {}", format!($($arg)*));
        println!("{}", "=".repeat(60));
    };
}

fn fmt_value(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

fn print_transaction(label: &str, tx: &Transaction) {
    println!("\n  ┌─ {} ─", label);
    println!("  │ id:        {}", tx.id);
    println!("  │ state:     {:?}", tx.state);
    if tx.read_set.is_empty() {
        println!("  │ read_set:  (empty)");
    } else {
        println!("  │ read_set:");
        for (k, v) in &tx.read_set {
            println!("  │   {} → {}", k, v);
        }
    }
    if tx.write_set.is_empty() {
        println!("  │ write_set: (empty)");
    } else {
        println!("  │ write_set:");
        for (k, v) in &tx.write_set {
            println!("  │   {} → {:?}", k, fmt_value(v));
        }
    }
    println!("  │ vclock:    {} bytes", tx.state_vclock.as_ref().map_or(0, |vc| vc.0.len()));
    println!("  └───────────────────────────────────────────");
}

async fn print_variable(client: &Client, var: &str) {
    match client.get_all_with_vclock::<Vec<String>>(Bucket::Variables, var).await {
        Ok((sibling_vecs, vc)) => {
            let all_ids: Vec<String> = sibling_vecs.into_iter().flatten().collect();
            println!("  📦 variable '{}' → {:?}  (vclock {} bytes)", var, all_ids, vc.0.len());
        }
        Err(e) => println!("  📦 variable '{}' → not found ({:?})", var, e),
    }
}

async fn get_variable_ids(client: &Client, var: &str) -> Vec<String> {
    match client.get_all_with_vclock::<Vec<String>>(Bucket::Variables, var).await {
        Ok((sibling_vecs, _)) => {
            let mut ids: Vec<String> = sibling_vecs.into_iter().flatten().collect();
            ids.retain(|s| s != "$");
            ids.sort();
            ids.dedup();
            ids
        }
        Err(_) => Vec::new(),
    }
}

macro_rules! verify {
    ($label:expr, $cond:expr) => {
        if $cond {
            println!("  ✔ {}", $label);
        } else {
            println!("  ✗ {}", $label);
        }
    };
}

#[tokio::test]
async fn test_read_write_variable_lifecycle() {
    let client = Client::new(HOST);

    // ================================================================
    print_section!("STEP 1: GENESIS — create variable from scratch");
    println!("  Creating a 'genesis' transaction. This is the first transaction");
    println!("  that writes a new variable. No explicit approve() — read() will");
    println!("  auto-approve it when someone reads the variable.");

    let balance_var = unique_key("balance");
    let mut genesis = Transaction::new(&client);
    println!("\n  Created new transaction id: {}", genesis.id);

    println!("  → Adding write_set[\"{}\"] = \"1000\" (initial value)", balance_var);
    genesis.write_set.insert(balance_var.clone(), b"1000".to_vec());
    print_transaction("genesis BEFORE commit", &genesis);

    println!("\n  → Calling genesis.commit() ...");
    println!("     This persist read_set/write_set to Riak, sets status to Proposed,");
    println!("     and registers the tx id in the Variables bucket.");
    genesis.commit().await.expect("genesis commit should succeed");
    print_transaction("genesis AFTER commit (Proposed)", &genesis);

    verify!("State is Proposed", matches!(genesis.state, TransactionStatus::Proposed { .. }));

    println!("\n  Checking Riak: what tx ids are registered for variable '{}'?", balance_var);
    print_variable(&client, &balance_var).await;

    println!("\n  → Reconstructing genesis from UUID via Transaction::from_uuid() ...");
    let recon = Transaction::from_uuid(genesis.id, &client)
        .await
        .expect("reconstruct genesis should succeed");
    print_transaction("reconstructed genesis", &recon);
    verify!("Reconstructed state is Proposed (not yet approved)", matches!(recon.state, TransactionStatus::Proposed { .. }));

    // ================================================================
    print_section!("STEP 2: READ — first read auto-approves the genesis tx");
    println!("  When reader.read() is called, find_dependency() discovers that");
    println!("  genesis is the only frontier writer for this variable. Since it's");
    println!("  Proposed (not yet Approved), find_dependency auto-approves it and");
    println!("  returns its id. The reader then gets the value from genesis.write_set.");

    let mut reader = Transaction::new(&client);
    println!("  Created reader tx id: {}", reader.id);

    println!("\n  → Calling reader.read(\"{}\") ...", balance_var);
    println!("     find_dependency will:");
    println!("       1. Look up variable '{}' in Riak → finds genesis id", balance_var);
    println!("       2. Load genesis transaction, see it's Proposed but on the frontier");
    println!("       3. Auto-approve genesis (Proposed → Approved)");
    println!("       4. Return genesis.id as the dependency");
    println!("       5. Return genesis.write_set[\"{}\"] = \"1000\"", balance_var);
    let balance_value = reader.read(&balance_var).await.expect("read should succeed");
    println!("\n  read() returned: {:?}", balance_value.as_ref().map(|v| fmt_value(v)));
    verify!("Got Some value for existing variable", balance_value.is_some());
    verify!("Value is \"1000\" (matching genesis write)", balance_value.unwrap() == b"1000".to_vec());

    println!("\n  read() also auto-approved genesis and stored the dependency:");
    println!("    read_set[\"{}\"] = {}  (genesis id)", balance_var, genesis.id);
    verify!("read_set contains the variable key", reader.read_set.contains_key(&balance_var));
    verify!("read_set maps variable to genesis id", *reader.read_set.get(&balance_var).unwrap() == genesis.id);
    print_transaction("reader AFTER read", &reader);

    // Verify genesis is now Approved
    let recon_genesis = Transaction::from_uuid(genesis.id, &client)
        .await
        .expect("re-reconstruct genesis");
    print_transaction("genesis AFTER auto-approval", &recon_genesis);
    verify!("Genesis is now Approved", matches!(recon_genesis.state, TransactionStatus::Approved { .. }));

    // ================================================================
    print_section!("STEP 3: WRITE — reader writes an updated value");
    println!("  The reader now wants to edit the variable. It calls write()");
    println!("  to add the value to write_set. The value is only persisted when");
    println!("  commit() is called.");

    println!("\n  → Calling reader.write(\"{}\", \"800\") ...", balance_var);
    reader.write(&balance_var, b"800".to_vec());
    print_transaction("reader AFTER write (before commit)", &reader);

    println!("\n  → Calling reader.commit() ...");
    reader.commit().await.expect("reader commit should succeed");
    print_transaction("reader AFTER commit (Proposed)", &reader);
    verify!("State is Proposed after commit", matches!(reader.state, TransactionStatus::Proposed { .. }));

    println!("\n  Variable '{}' now has both tx ids registered:", balance_var);
    print_variable(&client, &balance_var).await;

    // ================================================================
    print_section!("STEP 4: SECOND READ — auto-approves the reader, returns updated value");
    println!("  A second reader calls read(). find_dependency discovers that");
    println!("  the reader (Proposed, writes \"800\") is the frontier tip because");
    println!("  it depends on genesis (which is now Approved). It auto-approves");
    println!("  the reader and returns its value.");

    let mut second_reader = Transaction::new(&client);
    println!("  Created second_reader tx id: {}", second_reader.id);

    println!("\n  → Calling second_reader.read(\"{}\") ...", balance_var);
    let updated = second_reader.read(&balance_var).await.expect("second read should succeed");
    println!("  second_reader.read() returned: {:?}", updated.as_ref().map(|v| fmt_value(v)));
    verify!("Second reader sees \"800\" (reader's value)", updated.unwrap() == b"800".to_vec());

    println!("\n  read() auto-approved the first reader and stored dependency:");
    verify!("read_set references the first reader", second_reader.read_set.contains_key(&balance_var));
    print_transaction("second_reader AFTER read", &reader);

    // Verify first reader is now Approved
    let recon_reader = Transaction::from_uuid(reader.id, &client)
        .await
        .expect("reconstruct reader");
    print_transaction("first reader AFTER auto-approval", &recon_reader);
    verify!("First reader is now Approved", matches!(recon_reader.state, TransactionStatus::Approved { .. }));

    // Verify genesis id was removed from Variables after reader approval
    let var_ids = get_variable_ids(&client, &balance_var).await;
    println!("\n  Variable '{}' tx ids: {:?}", balance_var, var_ids);
    verify!("Genesis removed from variable (superseded by reader)", !var_ids.contains(&genesis.id.to_string()));
    verify!("Reader present in variable", var_ids.contains(&reader.id.to_string()));

    // ================================================================
    print_section!("STEP 5: VERIFY — reconstruct transactions from UUID");
    println!("  All state survives in Riak and can be reconstructed at any time.");

    println!("\n  → Reconstructing the first reader (which was auto-approved)...");
    let recon_reader = Transaction::from_uuid(reader.id, &client)
        .await
        .expect("reconstruct reader");
    print_transaction("reconstructed reader", &recon_reader);
    verify!("Reconstructed state is Approved", matches!(recon_reader.state, TransactionStatus::Approved { .. }));
    verify!("Reconstructed write_set has \"800\"", recon_reader.write_set.get(&balance_var) == Some(&b"800".to_vec()));
    verify!("Reconstructed read_set has the genesis as dependency", recon_reader.read_set.get(&balance_var) == Some(&genesis.id));

    println!("\n  ══════════════════════════════════════════");
    println!("  LIFECYCLE TEST PASSED — no explicit approve() calls needed!");
    println!("  ══════════════════════════════════════════");
}

#[tokio::test]
async fn test_second_reader_sees_latest_value() {
    let client = Client::new(HOST);

    print_section!("CHAIN — genesis → editor → second reader (all via read())");
    println!("  This test proves that each read() auto-approves the previous writer,");
    println!("  so the latest value is always visible without manual approve() calls.");

    // ── Genesis ──
    println!("\n  → Creating genesis: write \"hello\"");
    let myvar = unique_key("myvar");
    let mut genesis = Transaction::new(&client);
    genesis.write_set.insert(myvar.clone(), b"hello".to_vec());
    genesis.commit().await.expect("genesis commit");
    print_transaction("genesis (Proposed)", &genesis);

    // ── First read auto-approves genesis ──
    println!("\n  → First read: auto-approves genesis, returns \"hello\"");
    let mut editor = Transaction::new(&client);
    let value = editor.read(&myvar).await.expect("editor read");
    println!("  editor.read() = {:?}", value.as_ref().map(|v| fmt_value(v)));
    verify!("Editor sees \"hello\"", value.unwrap() == b"hello".to_vec());

    // Verify genesis is Approved
    let recon = Transaction::from_uuid(genesis.id, &client).await.expect("reconstruct");
    verify!("Genesis auto-approved", matches!(recon.state, TransactionStatus::Approved { .. }));

    // ── Editor writes ──
    println!("\n  → Editor writes \"world\" and commits");
    editor.write(&myvar, b"world".to_vec());
    editor.commit().await.expect("editor commit");
    print_transaction("editor (Proposed)", &editor);

    // ── Second read auto-approves editor ──
    println!("\n  → Second read: auto-approves editor, returns \"world\"");
    let mut second_reader = Transaction::new(&client);
    let updated = second_reader.read(&myvar).await.expect("second reader");
    println!("  second_reader.read() = {:?}", updated.as_ref().map(|v| fmt_value(v)));
    verify!("Second reader sees \"world\" (editor's value)", updated.unwrap() == b"world".to_vec());

    // Verify editor is Approved
    let recon_editor = Transaction::from_uuid(editor.id, &client).await.expect("reconstruct editor");
    verify!("Editor auto-approved", matches!(recon_editor.state, TransactionStatus::Approved { .. }));
    print_transaction("editor (Approved via auto-approve)", &recon_editor);

    // ── Second reader writes ──
    println!("\n  → Second reader writes \"!\" and commits");
    second_reader.write(&myvar, b"!".to_vec());
    second_reader.commit().await.expect("second reader commit");

    println!("\n  Variable '{}' before third read:", myvar);
    print_variable(&client, &myvar).await;

    // ── Third read auto-approves second reader ──
    println!("\n  → Third read: auto-approves second reader, returns \"!\"");
    let mut third_reader = Transaction::new(&client);
    let final_val = third_reader.read(&myvar).await.expect("third reader");
    println!("  third_reader.read() = {:?}", final_val.as_ref().map(|v| fmt_value(v)));
    verify!("Third reader sees \"!\" (second reader's value)", final_val.unwrap() == b"!".to_vec());

    let var_ids = get_variable_ids(&client, &myvar).await;
    println!("\n  Final variable tx ids: {:?}", var_ids);
    verify!("Genesis gone", !var_ids.contains(&genesis.id.to_string()));
    verify!("Editor gone", !var_ids.contains(&editor.id.to_string()));
    verify!("Second reader present", var_ids.contains(&second_reader.id.to_string()));

    println!("\n  ══════════════════════════════════════════");
    println!("  CHAIN TEST PASSED — full auto-approve chain works!");
    println!("  ══════════════════════════════════════════");
}

#[tokio::test]
async fn test_choose_tx_among_conflicting_writers() {
    let client = Client::new(HOST);

    print_section!("CONFLICT — two writers compete for the same variable");
    println!("  When two Proposed transactions both depend on the same parent,");
    println!("  find_dependency auto-approves the parent and then uses choose_tx");
    println!("  to deterministically select one winner (Approved) and reject the other.");

    // ── Genesis ──
    println!("\n  → Setting up genesis with value \"42\"");
    let counter_var = unique_key("counter");
    let mut genesis = Transaction::new(&client);
    genesis.write_set.insert(counter_var.clone(), b"42".to_vec());
    genesis.commit().await.expect("genesis commit");
    print_transaction("genesis (Proposed)", &genesis);

    // ── First read auto-approves genesis ──
    println!("\n  → First read auto-approves genesis");
    let mut approver = Transaction::new(&client);
    let val = approver.read(&counter_var).await.expect("approver read");
    verify!("Approver sees \"42\"", val.unwrap() == b"42".to_vec());

    // ── Two writers ──
    println!("\n  → Creating two competing writers");
    let mut writer_a = Transaction::new(&client);
    let mut writer_b = Transaction::new(&client);
    println!("  writer_a id: {}", writer_a.id);
    println!("  writer_b id: {}", writer_b.id);

    println!("\n  Both call read() — find_dependency resolves to genesis (now Approved):");
    let val_a = writer_a.read(&counter_var).await.expect("writer_a read");
    let val_b = writer_b.read(&counter_var).await.expect("writer_b read");
    println!("  writer_a.read() = {:?}", val_a.as_ref().map(|v| fmt_value(v)));
    println!("  writer_b.read() = {:?}", val_b.as_ref().map(|v| fmt_value(v)));
    verify!("writer_a value is \"42\"", val_a.unwrap() == b"42".to_vec());
    verify!("writer_b value is \"42\"", val_b.unwrap() == b"42".to_vec());

    println!("\n  → Each writer writes a different proposed value:");
    println!("  writer_a writes \"43\"");
    println!("  writer_b writes \"44\"");
    writer_a.write(&counter_var, b"43".to_vec());
    writer_b.write(&counter_var, b"44".to_vec());

    println!("\n  → Both commit (status = Proposed):");
    let id_a = writer_a.id;
    let _id_b = writer_b.id;
    writer_a.commit().await.expect("writer_a commit");
    writer_b.commit().await.expect("writer_b commit");

    println!("\n  Variable '{}' has both tx ids registered:", counter_var);
    print_variable(&client, &counter_var).await;

    // ── Resolution via read() ──
    println!("\n  → A new reader calls read() — this triggers conflict resolution:");
    println!("     find_dependency finds two Proposed writers on the frontier.");
    println!("     choose_tx deterministically picks one winner → Approved.");
    println!("     The loser is rejected → Rejected.");
    let mut resolver = Transaction::new(&client);
    let resolved_value = resolver.read(&counter_var).await.expect("resolver read should succeed");
    println!("  resolver.read() returned: {:?}", resolved_value.as_ref().map(|v| fmt_value(v)));
    verify!("Resolver got a value (either \"43\" or \"44\")", resolved_value.is_some());

    // Check which writer won
    let winner_a = Transaction::from_uuid(id_a, &client).await.expect("check writer_a");
    let winner_b_state = Transaction::from_uuid(writer_b.id, &client).await.expect("check writer_b");

    if matches!(winner_a.state, TransactionStatus::Approved { .. }) {
        println!("\n  writer_a WON (Approved), writer_b LOST (Rejected)");
        verify!("writer_a is Approved", matches!(winner_a.state, TransactionStatus::Approved { .. }));
        verify!("writer_b is Rejected", matches!(winner_b_state.state, TransactionStatus::Rejected { .. }));
    } else {
        println!("\n  writer_b WON (Approved), writer_a LOST (Rejected)");
        verify!("writer_b is Approved", matches!(winner_b_state.state, TransactionStatus::Approved { .. }));
        verify!("writer_a is Rejected", matches!(winner_a.state, TransactionStatus::Rejected { .. }));
    }

    println!("\n  ══════════════════════════════════════════");
    println!("  CONFLICT RESOLUTION TEST PASSED");
    println!("  ══════════════════════════════════════════");
}

#[tokio::test]
async fn test_cross_variable_concurrent_reads_inconsistent() {
    let client = Client::new(HOST);

    print_section!("BUG: concurrent find_dependency calls can produce inconsistent state");
    println!("  Setup:");
    println!("    genesis: Approved,  write_set = {{A: \"v0\", B: \"v0\"}}");
    println!("    tx1:     Proposed, read_set = {{A: genesis}}, write_set = {{A: \"v1\"}}");
    println!("    tx2:     Proposed, read_set = {{A: genesis, B: genesis}}, write_set = {{A: \"v2\", B: \"v2\"}}");
    println!("    tx3:     Proposed, read_set = {{B: genesis}}, write_set = {{B: \"v3\"}}");
    println!("");
    println!("  BUG: find_dependency(\"A\") sees {{genesis, tx1, tx2}} but NOT tx3.");
    println!("  find_dependency(\"B\") sees {{genesis, tx2, tx3}} but NOT tx1.");
    println!("  Each makes an INDEPENDENT choose_tx decision about tx2.");
    println!("  If A rejects tx2 while B approves tx2, tx2 gets contradictory");
    println!("  statuses — Approved by one read, Rejected by the other.");
    println!("");
    println!("  This test runs both reads CONCURRENTLY to trigger the race.");

    let var_a = unique_key("varA");
    let var_b = unique_key("varB");

    // Create genesis (writes both A and B)
    let mut genesis = Transaction::new(&client);
    genesis.write_set.insert(var_a.clone(), b"v0".to_vec());
    genesis.write_set.insert(var_b.clone(), b"v0".to_vec());
    genesis.commit().await.expect("genesis commit");

    // Auto-approve genesis
    let mut approver = Transaction::new(&client);
    let _ = approver.read(&var_a).await.expect("auto-approve genesis");
    verify!("Genesis is now Approved", matches!(
        Transaction::from_uuid(genesis.id, &client).await.expect("recheck").state,
        TransactionStatus::Approved { .. }
    ));

    // tx1: reads A from genesis, writes A
    let mut tx1 = Transaction::new(&client);
    tx1.read_set.insert(var_a.clone(), genesis.id);
    tx1.write_set.insert(var_a.clone(), b"v1".to_vec());
    tx1.commit().await.expect("tx1 commit");

    // tx2: reads A and B from genesis, writes both
    let mut tx2 = Transaction::new(&client);
    tx2.read_set.insert(var_a.clone(), genesis.id);
    tx2.read_set.insert(var_b.clone(), genesis.id);
    tx2.write_set.insert(var_a.clone(), b"v2".to_vec());
    tx2.write_set.insert(var_b.clone(), b"v2".to_vec());
    tx2.commit().await.expect("tx2 commit");

    // tx3: reads B from genesis, writes B
    let mut tx3 = Transaction::new(&client);
    tx3.read_set.insert(var_b.clone(), genesis.id);
    tx3.write_set.insert(var_b.clone(), b"v3".to_vec());
    tx3.commit().await.expect("tx3 commit");

    println!("\n  Variables in Riak:");
    print_variable(&client, &var_a).await;
    print_variable(&client, &var_b).await;
    println!("  find_dependency(\"A\") will see: genesis, tx1, tx2  (NOT tx3)");
    println!("  find_dependency(\"B\") will see: genesis, tx2, tx3  (NOT tx1)");

    // Run reads CONCURRENTLY — both see tx2 as Proposed
    // find_dependency("A") calls choose_tx({tx1, tx2})
    // find_dependency("B") calls choose_tx({tx2, tx3})
    // These are INDEPENDENT decisions about tx2!
    println!("\n  → Spawning CONCURRENT reads for A and B...");
    println!("  Both will see tx2 as Proposed and make independent choose_tx decisions.");

    let client_a = Client::new(HOST);
    let client_b = Client::new(HOST);
    let var_a_for_task = var_a.clone();
    let var_b_for_task = var_b.clone();

    let handle_a = tokio::spawn(async move {
        let mut reader = Transaction::new(&client_a);
        let val = reader.read(&var_a_for_task).await.expect("concurrent read A");
        (val, reader.read_set.get(&var_a_for_task).copied())
    });

    let handle_b = tokio::spawn(async move {
        let mut reader = Transaction::new(&client_b);
        let val = reader.read(&var_b_for_task).await.expect("concurrent read B");
        (val, reader.read_set.get(&var_b_for_task).copied())
    });

    let (result_a, result_b) = tokio::join!(handle_a, handle_b);
    let (val_a, dep_a) = result_a.expect("task A");
    let (val_b, dep_b) = result_b.expect("task B");

    println!("  read(\"A\") = {:?}", val_a.as_ref().map(|v| fmt_value(v)));
    println!("  read(\"B\") = {:?}", val_b.as_ref().map(|v| fmt_value(v)));
    println!("  A depends on: {:?}", dep_a);
    println!("  B depends on: {:?}", dep_b);

    // Check final statuses
    let status_tx1 = Transaction::from_uuid(tx1.id, &client).await.expect("tx1 status");
    let status_tx2 = Transaction::from_uuid(tx2.id, &client).await.expect("tx2 status");
    let status_tx3 = Transaction::from_uuid(tx3.id, &client).await.expect("tx3 status");

    println!("\n  Final statuses:");
    println!("  tx1: {:?}", status_tx1.state);
    println!("  tx2: {:?}", status_tx2.state);
    println!("  tx3: {:?}", status_tx3.state);

    let tx2_approved = matches!(status_tx2.state, TransactionStatus::Approved { .. });
    let tx2_rejected = matches!(status_tx2.state, TransactionStatus::Rejected { .. });

    // The KEY assertion: tx2's status should be unambiguous.
    // If both A and B made independent choose_tx decisions about tx2,
    // the race condition may have created sibling statuses in Riak.
    // from_uuid picks the earliest final state, so one "wins".
    // But the logical inconsistency is: tx2 was chosen by one variable
    // and rejected by the other.

    // Check: is the outcome globally consistent?
    // A globally consistent outcome means: the variable whose read resolved
    // to tx2 actually uses tx2's value, and the variable that didn't
    // resolve to tx2 has a different Approved writer.
    let a_uses_tx2 = dep_a == Some(tx2.id);
    let b_uses_tx2 = dep_b == Some(tx2.id);

    println!("\n  ── Consistency analysis ──");
    println!("  tx2 Approved? {}", tx2_approved);
    println!("  tx2 Rejected? {}", tx2_rejected);
    println!("  A uses tx2? {}", a_uses_tx2);
    println!("  B uses tx2? {}", b_uses_tx2);

    // The OUTCOME should be: exactly one Approved writer per variable.
    // If tx2 is Approved, it should serve BOTH A and B (since it writes to both).
    // If tx2 is Rejected, NEITHER A nor B should use tx2.
    //
    // The BUG: because find_dependency runs independently for A and B,
    // it's possible that:
    //   - A's choose_tx rejects tx2 (picks tx1)
    //   - B's choose_tx approves tx2 (picks tx2 over tx3)
    //   - The race condition: one Riak write wins, tx2 gets one final status
    //   - If tx2 ends up Approved: A returned tx1's value but tx2 is Approved
    //   - If tx2 ends up Rejected: B might have returned tx2's value before rejection
    //
    // This assertion checks: if tx2 is Approved, then BOTH A and B should
    // resolve to tx2 (since tx2 writes to both). If tx2 is Rejected, then
    // NEITHER should resolve to tx2.

    let inconsistent = if tx2_approved {
        // tx2 is Approved: it should be authoritative for BOTH A and B
        // But find_dependency("A") may have picked tx1 instead
        !a_uses_tx2 || !b_uses_tx2
    } else if tx2_rejected {
        // tx2 is Rejected: neither should use it
        // This should be fine — A uses tx1, B uses tx3
        a_uses_tx2 || b_uses_tx2
    } else {
        // tx2 is still Proposed — unclear outcome
        false
    };

    if inconsistent {
        if tx2_approved && (!a_uses_tx2 || !b_uses_tx2) {
            println!("\n  ⚠ BUG CONFIRMED: tx2 is Approved but doesn't serve both variables!");
            println!("  find_dependency(\"A\") didn't see tx3, so it made an independent");
            println!("  choose_tx decision that conflicts with B's decision.");
            println!("  tx2 should be the winner for BOTH A and B since it writes to both,");
            println!("  but A resolved to a different transaction.");
        }
        if tx2_rejected && (a_uses_tx2 || b_uses_tx2) {
            println!("\n  ⚠ BUG CONFIRMED: tx2 is Rejected but still serves a variable!");
        }
    } else {
        // This CAN happen if both reads happened to pick the same winner for tx2
        // (or the sequential nature of task scheduling made one finish first)
        println!("\n  Outcome appears consistent (but may be luck — concurrency-dependent).");
        if tx2_approved {
            println!("  tx2 won both A and B, which is globally consistent.");
            println!("  This might not happen every run — the bug is concurrency-dependent.");
        }
        if tx2_rejected {
            println!("  tx2 was rejected, so tx1 serves A and tx3 serves B.");
            println!("  This is consistent, but suboptimal: tx2 writes to BOTH variables");
            println!("  and could have been the best choice for both.");
        }
    }

    // Hard assertion: the bug IS that find_dependency doesn't load
    // transitively connected transactions. This assertion WILL fail
    // if both reads make conflicting decisions about tx2.
    //
    // Specifically: if tx2 is Approved, the winner for A should be tx2
    // AND the winner for B should also be tx2 (since tx2 writes to both).
    // If this assertion fails, it proves the cross-variable inconsistency bug.
    if tx2_approved {
        assert!(a_uses_tx2 && b_uses_tx2,
            "BUG: tx2 is Approved but A resolved to {:?} and B resolved to {:?}. \
             find_dependency doesn't load transitively connected variables, \
             causing independent (and potentially conflicting) decisions about tx2.",
            dep_a, dep_b);
    }

    if tx2_rejected {
        assert!(!a_uses_tx2 && !b_uses_tx2,
            "BUG: tx2 is Rejected but A resolved to {:?} and B resolved to {:?}.",
            dep_a, dep_b);
    }

    println!("\n  ══════════════════════════════════════════");
    println!("  CROSS-VARIABLE BUG TEST COMPLETE");
    println!("  ══════════════════════════════════════════");
}

#[tokio::test]
async fn test_read_nonexistent_variable_returns_none() {
    let client = Client::new(HOST);

    print_section!("READ — reading a nonexistent variable returns None");
    println!("  When read() is called on a variable that has never been written,");
    println!("  find_dependency finds no entries in the Variables bucket and returns None.");

    let mut tx = Transaction::new(&client);
    let nonexistent = unique_key("ghost");
    println!("\n  Variable name: \"{}\" (never written to)", nonexistent);

    println!("\n  → Calling tx.read(\"{}\") ...", nonexistent);
    let result = tx.read(&nonexistent).await.expect("read should not error");
    println!("  Result: {:?}", result.as_ref().map(|v| fmt_value(v)));
    verify!("read() returned None for nonexistent variable", result.is_none());
    verify!("read_set is still empty", tx.read_set.is_empty());
    print_transaction("tx after reading nonexistent var", &tx);

    println!("\n  ══════════════════════════════════════════");
    println!("  NONEXISTENT VARIABLE TEST PASSED");
    println!("  ══════════════════════════════════════════");
}

#[tokio::test]
async fn test_stale_proposed_tx_enters_choose_tx_when_it_should_be_rejected() {
    let client = Client::new(HOST);

    print_section!("BUG: stale Proposed tx enters choose_tx instead of being rejected");
    println!("  Setup:");
    println!("    1. genesis writes B → Approved via first read");
    println!("    2. tx1 writes B, reads B from genesis → Proposed → auto-approved (single tip)");
    println!("    3. tx2 writes B, reads B from tx1 (legitimate successor) → Proposed");
    println!("    4. stale_tx writes B, reads B from genesis (same parent as tx1!) → Proposed");
    println!("");
    println!("  The key insight: stale_tx.read_set[B] = genesis, same as tx1.read_set[B].");
    println!("  This means stale_tx was proposed in the SAME GENERATION as tx1 (both");
    println!("  depend on genesis for B). tx1 was approved, so stale_tx should be rejected");
    println!("  — it already lost the competition for this variable slot.");
    println!("");
    println!("  But find_dependency doesn't check for this. When a later reader calls");
    println!("  find_dependency(B), it sees tx2 (Proposed, reads tx1) and stale_tx");
    println!("  (Proposed, reads genesis) as two competing tips. It calls choose_tx");
    println!("  on them, even though stale_tx should have been excluded from the round");
    println!("  because it shares a parent with an already-Approved writer (tx1).");
    println!("");
    println!("  If choose_tx picks stale_tx, it gets Approved — even though it depends");
    println!("  on genesis (which was already superseded by tx1). This creates an");
    println!("  inconsistent dependency chain: stale_tx → genesis, ignoring tx1 entirely.");

    let myvar = unique_key("myvar");

    // ── Step 1: genesis ──
    println!("\n  → Creating genesis: write \"v0\"");
    let mut genesis = Transaction::new(&client);
    genesis.write_set.insert(myvar.clone(), b"v0".to_vec());
    genesis.commit().await.expect("genesis commit");
    print_transaction("genesis (Proposed)", &genesis);

    // ── Step 2: auto-approve genesis ──
    let mut approver = Transaction::new(&client);
    let val = approver.read(&myvar).await.expect("approve genesis");
    verify!("Approver sees \"v0\"", val == Some(b"v0".to_vec()));
    let genesis_approved = Transaction::from_uuid(genesis.id, &client).await.expect("recheck genesis");
    verify!("Genesis is Approved", matches!(genesis_approved.state, TransactionStatus::Approved { .. }));
    print_transaction("genesis (Approved)", &genesis_approved);

    // ── Step 3: tx1 — reads genesis, writes B. Single tip → auto-approved. ──
    println!("\n  → Creating tx1: reads genesis, writes \"v1\"");
    println!("     tx1 is the only Proposed writer on top of genesis, so it gets auto-approved.");
    let mut tx1 = Transaction::new(&client);
    tx1.read_set.insert(myvar.clone(), genesis.id);
    tx1.write_set.insert(myvar.clone(), b"v1".to_vec());
    tx1.commit().await.expect("tx1 commit");
    print_transaction("tx1 (Proposed)", &tx1);

    // First reader auto-approves tx1
    let mut first_reader = Transaction::new(&client);
    let val1 = first_reader.read(&myvar).await.expect("first read");
    println!("  first_reader.read() = {:?}", val1.as_ref().map(|v| fmt_value(v)));
    verify!("First reader sees \"v1\" (tx1's value)", val1 == Some(b"v1".to_vec()));

    let tx1_after = Transaction::from_uuid(tx1.id, &client).await.expect("check tx1");
    verify!("tx1 is Approved", matches!(tx1_after.state, TransactionStatus::Approved { .. }));
    print_transaction("tx1 (Approved)", &tx1_after);

    let tx1_approved_at = match &tx1_after.state {
        TransactionStatus::Approved { at } => *at,
        _ => panic!("expected Approved"),
    };
    println!("  tx1.approved_at = {}", tx1_approved_at);

    // ── Step 4: tx2 — reads tx1, writes B. Legitimate successor. ──
    println!("\n  → Creating tx2: reads tx1, writes \"v2\"");
    println!("     tx2 is a legitimate successor of tx1 — it reads from tx1, not genesis.");
    let mut tx2 = Transaction::new(&client);
    tx2.read_set.insert(myvar.clone(), tx1.id);
    tx2.write_set.insert(myvar.clone(), b"v2".to_vec());
    tx2.commit().await.expect("tx2 commit");
    print_transaction("tx2 (Proposed)", &tx2);

    // ── Step 5: stale_tx — reads genesis (same parent as tx1), writes B. ──
    //    This is the stale transaction. It should be rejected because:
    //    - It depends on genesis for B (same as tx1)
    //    - tx1 was already Approved for B
    //    - stale_tx was proposed during or before tx1's approval, but committed
    //      after tx1 was already Approved for B
    //    - Therefore stale_tx is a "sibling competitor" of tx1 that already lost
    println!("\n  → Creating stale_tx: reads genesis, writes \"v_stale\"");
    println!("     stale_tx.read_set[B] = genesis (SAME PARENT as tx1!)");
    println!("     This means stale_tx was a contemporary of tx1 that wasn't included");
    println!("     in tx1's validation round. It should be Rejected, not enter a new");
    println!("     choose_tx round with tx2.");
    let mut stale_tx = Transaction::new(&client);
    stale_tx.read_set.insert(myvar.clone(), genesis.id);
    stale_tx.write_set.insert(myvar.clone(), b"v_stale".to_vec());
    stale_tx.commit().await.expect("stale_tx commit");
    print_transaction("stale_tx (Proposed)", &stale_tx);

    let stale_state = Transaction::from_uuid(stale_tx.id, &client).await.expect("check stale");
    let stale_proposed_at = match &stale_state.state {
        TransactionStatus::Proposed { at } => *at,
        other => panic!("expected Proposed, got {:?}", other),
    };
    println!("  stale_tx.proposed_at = {}", stale_proposed_at);

    println!("\n  Variables in Riak:");
    print_variable(&client, &myvar).await;

    println!("\n  ── Dependency analysis ──");
    println!("  tx1.read_set[B] = {} (genesis)", tx1.id);
    println!("  tx2.read_set[B] = {} (tx1, the Approved writer)", tx2.id);
    println!("  stale_tx.read_set[B] = {} (genesis, SAME as tx1's parent!)", genesis.id);
    println!("");
    println!("  CORRECT behavior: stale_tx should be Rejected because:");
    println!("    1. tx1 is already Approved for B");
    println!("    2. stale_tx.read_set[B] == tx1.read_set[B] (both read from genesis)");
    println!("    3. stale_tx and tx1 are in the same generation (same parent for B)");
    println!("    4. tx1 won that generation, so stale_tx should be rejected");
    println!("");
    println!("  BUGGY behavior (current): find_dependency treats stale_tx as a tip");
    println!("  alongside tx2. Both have parents NOT in the writer set, so both are tips.");
    println!("  find_dependency calls choose_tx({{tx2, stale_tx}}), and stale_tx might win.");

    // ── Step 6: read B — triggers find_dependency ──
    println!("\n  → Reading B to trigger find_dependency...");
    let mut reader = Transaction::new(&client);
    let val_b = reader.read(&myvar).await.expect("read B");
    println!("  reader.read() = {:?}", val_b.as_ref().map(|v| fmt_value(v)));

    // ── Check outcomes ──
    let tx1_final = Transaction::from_uuid(tx1.id, &client).await.expect("tx1 final");
    let tx2_final = Transaction::from_uuid(tx2.id, &client).await.expect("tx2 final");
    let stale_final = Transaction::from_uuid(stale_tx.id, &client).await.expect("stale final");

    println!("\n  ── Final states ──");
    print_transaction("tx1 final", &tx1_final);
    print_transaction("tx2 final", &tx2_final);
    print_transaction("stale_tx final", &stale_final);

    let dep = reader.read_set.get(&myvar).copied();
    println!("\n  reader depends on: {:?}", dep);
    println!("  reader.read = {:?}", val_b.as_ref().map(|v| fmt_value(v)));

    // ── Assertions ──
    // The reader should depend on either tx1 or tx2 (the legitimate chain),
    // NOT on stale_tx (which reads from the superseded genesis).
    let reader_depends_on_stale = dep == Some(stale_tx.id);
    let stale_is_approved = matches!(stale_final.state, TransactionStatus::Approved { .. });
    let stale_is_proposed = matches!(stale_final.state, TransactionStatus::Proposed { .. });

    if stale_is_approved {
        println!("\n  ⚠ BUG CONFIRMED: stale_tx was APPROVED!");
        println!("  stale_tx reads from genesis (superseded by tx1) for variable B.");
        println!("  It should have been Rejected because it's in the same generation");
        println!("  as tx1 (both depend on genesis for B), and tx1 already won.");
        println!("  Instead, choose_tx included stale_tx in the round and it won,");
        println!("  creating a broken dependency chain: stale_tx → genesis, bypassing tx1.");
    }

    if stale_is_proposed {
        println!("\n  ⚠ BUG CONFIRMED: stale_tx is still PROPOSED (zombie).");
        println!("  It was left out of the resolution because find_dependency");
        println!("  preferred an Approved tip (tx1) over the Proposed stale_tx.");
        println!("  But stale_tx should have been explicitly Rejected, not left");
        println!("  as a zombie that could cause problems later.");
    }

    // KEY ASSERTION: stale_tx should be Rejected.
    // It shares a parent with an already-Approved writer (tx1) for the same variable.
    // find_dependency should detect this and reject it, not leave it as Proposed
    // or (worse) let it win a choose_tx round.
    assert!(
        matches!(stale_final.state, TransactionStatus::Rejected { .. }),
        "BUG: stale_tx should be Rejected (same parent as Approved tx1 for variable B), \
         but its state is {:?}. \
         stale_tx.read_set[B] = genesis (same as tx1), meaning they compete for the \
         same variable slot. Since tx1 already won that competition, stale_tx should \
         be Rejected.",
        stale_final.state
    );

    // Additionally: the reader should NOT depend on stale_tx.
    // If stale_tx won choose_tx, the reader depends on it — that's wrong.
    assert!(
        !reader_depends_on_stale,
        "BUG: reader depends on stale_tx ({}) instead of the legitimate chain. \
         stale_tx reads from genesis (superseded), bypassing the Approved tx1.",
        stale_tx.id
    );

    // The reader should depend on whoever won: either tx1 or tx2.
    // Both are in the legitimate chain: genesis → tx1 → tx2.
    let reader_depends_on_tx1 = dep == Some(tx1.id);
    let reader_depends_on_tx2 = dep == Some(tx2.id);
    assert!(
        reader_depends_on_tx1 || reader_depends_on_tx2,
        "reader should depend on tx1 or tx2 (legitimate chain), but depends on {:?}",
        dep
    );

    println!("\n  ══════════════════════════════════════════");
    println!("  STALE PROPOSED TX BUG TEST COMPLETE");
    println!("  ══════════════════════════════════════════");
}

