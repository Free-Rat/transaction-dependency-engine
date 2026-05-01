#![cfg(test)]

use crate::riak::client::{Client, Bucket};
use crate::transaction::transaction::{Transaction, TransactionStatus};
use rand::random;
use uuid::Uuid;

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
    println!("  that writes a new variable. It has no dependencies (read_set empty).");

    let balance_var = unique_key("balance");
    println!("\n  Picking unique variable name: \"{}\"", balance_var);

    let mut genesis = Transaction::new(&client);
    println!("  Created new transaction id: {}", genesis.id);
    println!("  State right after Transaction::new(): {:?}", genesis.state);

    println!("\n  → Adding write_set[\"{}\"] = \"1000\" (initial value)", balance_var);
    genesis.write_set.insert(balance_var.clone(), b"1000".to_vec());
    print_transaction("genesis BEFORE commit", &genesis);

    println!("\n  → Calling genesis.commit() ...");
    println!("     This will:");
    println!("       1. Persist read_set to Riak bucket ReadSets");
    println!("       2. Persist write_set to Riak bucket WriteSets");
    println!("       3. Change status from Created → Proposed");
    println!("       4. Register this tx id in Variables bucket for each write_set key");
    genesis.commit().await.expect("genesis commit should succeed");
    print_transaction("genesis AFTER commit", &genesis);
    verify!("State is Proposed after commit", matches!(genesis.state, TransactionStatus::Proposed { .. }));

    println!("\n  → Calling genesis.approve() ...");
    println!("     This will:");
    println!("       1. Change status from Proposed → Approved");
    println!("       2. Remove parent tx ids from Variables for each key in read_set");
    println!("       (genesis has empty read_set, so no parents to remove)");
    genesis.approve().await.expect("genesis approve should succeed");
    print_transaction("genesis AFTER approve", &genesis);
    verify!("State is Approved after approve", matches!(genesis.state, TransactionStatus::Approved { .. }));

    println!("\n  Checking Riak: what tx ids are registered for variable '{}'?", balance_var);
    print_variable(&client, &balance_var).await;

    println!("\n  → Reconstructing genesis from UUID via Transaction::from_uuid() ...");
    println!("     This fetches read_set, write_set, and status from Riak and rebuilds the object.");
    let recon = Transaction::from_uuid(genesis.id, &client)
        .await
        .expect("reconstruct genesis should succeed");
    print_transaction("reconstructed genesis", &recon);
    verify!("Reconstructed id matches", recon.id == genesis.id);
    verify!("Reconstructed state is Approved", matches!(recon.state, TransactionStatus::Approved { .. }));
    verify!("Reconstructed write_set has correct value", recon.write_set.get(&balance_var) == Some(&b"1000".to_vec()));

    // ================================================================
    print_section!("STEP 2: READ — new tx reads the variable, finds genesis as dependency");
    println!("  When a new tx reads a variable, find_dependency() is called.");
    println!("  It looks up which tx ids are registered for that variable in Riak,");
    println!("  then determines the 'frontline' transaction to read from.");

    let mut reader = Transaction::new(&client);
    println!("  Created reader tx id: {}", reader.id);

    println!("\n  → Calling reader.read(\"{}\") ...", balance_var);
    println!("     find_dependency will:");
    println!("       1. GET variable '{}' from Riak → finds genesis tx id", balance_var);
    println!("       2. Load genesis transaction, see it's Approved");
    println!("       3. Since genesis has no read_set entries for this variable,");
    println!("          it's a valid frontier tx → return genesis.id");
    println!("       4. Load genesis transaction to get its write_set value");
    let balance_value = reader.read(&balance_var).await.expect("read should succeed");
    println!("\n  read() returned: {:?}", balance_value.as_ref().map(|v| fmt_value(v)));
    verify!("Got Some value for existing variable", balance_value.is_some());
    verify!("Value is \"1000\" (matching genesis write)", balance_value.unwrap() == b"1000".to_vec());

    println!("\n  The read() call also populated reader.read_set:");
    println!("    read_set[\"{}\"] = {}  (the genesis tx id)", balance_var, genesis.id);
    verify!("read_set contains the variable key", reader.read_set.contains_key(&balance_var));
    verify!("read_set maps variable to genesis id", *reader.read_set.get(&balance_var).unwrap() == genesis.id);
    print_transaction("reader AFTER read", &reader);

    // ================================================================
    print_section!("STEP 3: WRITE — reader writes an updated value");
    println!("  Now the reader wants to edit the variable. It calls write()");
    println!("  which adds the value to write_set. The actual value is only");
    println!("  persisted to Riak when commit() is called.");

    println!("\n  → Calling reader.write(\"{}\", \"800\") ...", balance_var);
    reader.write(&balance_var, b"800".to_vec());
    print_transaction("reader AFTER write (before commit)", &reader);

    println!("\n  → Calling reader.commit() ...");
    println!("     This will:");
    println!("       1. Persist read_set and write_set to Riak");
    println!("       2. Change status from Created → Proposed");
    println!("       3. Register reader tx id in Variables for balance_var");
    reader.commit().await.expect("reader commit should succeed");
    print_transaction("reader AFTER commit", &reader);
    verify!("State is Proposed after commit", matches!(reader.state, TransactionStatus::Proposed { .. }));

    println!("\n  Checking Riak: variable '{}' now has both tx ids registered:", balance_var);
    print_variable(&client, &balance_var).await;

    // ================================================================
    print_section!("STEP 4: APPROVE — approve the reader tx; genesis removed from variable");
    println!("  When a tx is approved, its read_set parents are removed from");
    println!("  the Variables bucket. This is because the approved tx supersedes");
    println!("  the parent — only the latest transaction on the frontier matters.");

    println!("\n  → Calling reader.approve() ...");
    println!("     This will:");
    println!("       1. Change status from Proposed → Approved");
    println!("       2. For each (variable, parent_id) in read_set:");
    println!("          Remove parent_id from Variables[variable]");
    println!("          (genesis id will be removed from Variables[\"{}\"])", balance_var);
    reader.approve().await.expect("reader approve should succeed");
    print_transaction("reader AFTER approve", &reader);
    verify!("State is Approved after approve", matches!(reader.state, TransactionStatus::Approved { .. }));

    let var_ids = get_variable_ids(&client, &balance_var).await;
    println!("\n  Variable '{}' tx ids after approve: {:?}", balance_var, var_ids);
    verify!("Genesis id removed from variable", !var_ids.contains(&genesis.id.to_string()));
    verify!("Reader id still present in variable", var_ids.contains(&reader.id.to_string()));

    // ================================================================
    print_section!("STEP 5: VERIFY — reconstruct reader tx from UUID");
    println!("  Prove that all state survived in Riak by reconstructing from UUID.");

    println!("\n  → Calling Transaction::from_uuid({}) ...", reader.id);
    let recon_reader = Transaction::from_uuid(reader.id, &client)
        .await
        .expect("reconstruct reader should succeed");
    print_transaction("reconstructed reader", &recon_reader);
    verify!("Reconstructed state is Approved", matches!(recon_reader.state, TransactionStatus::Approved { .. }));
    verify!("Reconstructed write_set has \"800\"", recon_reader.write_set.get(&balance_var) == Some(&b"800".to_vec()));
    verify!("Reconstructed read_set has variable key", recon_reader.read_set.contains_key(&balance_var));

    println!("\n  ══════════════════════════════════════════");
    println!("  LIFECYCLE TEST PASSED");
    println!("  ══════════════════════════════════════════");
}

#[tokio::test]
async fn test_second_reader_reads_updated_value() {
    let client = Client::new(HOST);

    print_section!("FULL CHAIN — genesis → editor → second reader");
    println!("  This test demonstrates that when a variable is updated,");
    println!("  a new reader sees the LATEST approved value, not the old one.");

    // ── Genesis ──
    println!("\n  → Creating genesis transaction: write \"hello\" → {}", unique_key("myvar"));
    let myvar = unique_key("myvar");
    let mut genesis = Transaction::new(&client);
    genesis.write_set.insert(myvar.clone(), b"hello".to_vec());
    genesis.commit().await.expect("genesis commit");
    genesis.approve().await.expect("genesis approve");
    print_transaction("genesis (Approved, value=\"hello\")", &genesis);
    println!("\n  Genesis is now the frontier for variable '{}'.", myvar);

    // ── First edit ──
    println!("\n  → Creating editor transaction: reads → writes \"world\"");
    let mut editor = Transaction::new(&client);
    println!("  editor.read() on '{}' — find_dependency resolves to genesis", myvar);
    let value = editor.read(&myvar).await.expect("editor read should succeed");
    println!("  editor read value: {:?} (from genesis)", value.as_ref().map(|v| fmt_value(v)));
    verify!("Editor sees genesis value \"hello\"", value.unwrap() == b"hello".to_vec());

    println!("\n  editor.write() updates the value to \"world\"");
    editor.write(&myvar, b"world".to_vec());
    editor.commit().await.expect("editor commit");

    println!("\n  → editor.approve(): genesis should be removed from Variables[\"{}\"]", myvar);
    editor.approve().await.expect("editor approve");
    print_transaction("editor (Approved, value=\"world\")", &editor);
    println!("\n  Editor is now the frontier for variable '{}'.", myvar);

    // ── Second reader ──
    println!("\n  → Creating second_reader transaction");
    let mut second_reader = Transaction::new(&client);
    println!("  second_reader.read() — find_dependency should resolve to editor (not genesis)");

    let updated = second_reader.read(&myvar).await.expect("second reader should succeed");
    println!("  second_reader got value: {:?}", updated.as_ref().map(|v| fmt_value(v)));
    verify!("Second reader sees \"world\" (editor's value)", updated.unwrap() == b"world".to_vec());

    println!("\n    read_set[\"{}\"] = {}  (editor id, not genesis)", myvar, editor.id);
    verify!("read_set references editor id", *second_reader.read_set.get(&myvar).unwrap() == editor.id);
    print_transaction("second reader AFTER read", &second_reader);

    // ── Second reader also writes ──
    println!("\n  → second_reader writes \"!\" and commits");
    second_reader.write(&myvar, b"!".to_vec());
    second_reader.commit().await.expect("second reader commit");

    println!("\n  Variable '{}' has these tx ids before approve:", myvar);
    print_variable(&client, &myvar).await;

    println!("\n  → second_reader.approve(): editor removed, second_reader remains");
    second_reader.approve().await.expect("second reader approve");
    print_transaction("second reader (Approved, value=\"!\")", &second_reader);

    let var_ids = get_variable_ids(&client, &myvar).await;
    println!("\n  Final variable tx ids: {:?}", var_ids);
    verify!("Genesis gone from variable", !var_ids.contains(&genesis.id.to_string()));
    verify!("Editor gone from variable", !var_ids.contains(&editor.id.to_string()));
    verify!("Second reader present in variable", var_ids.contains(&second_reader.id.to_string()));

    let reconstructed = Transaction::from_uuid(second_reader.id, &client).await.expect("reconstruct");
    verify!("Reconstructed state is Approved", matches!(reconstructed.state, TransactionStatus::Approved { .. }));
    verify!("Reconstructed write_set has \"!\"", reconstructed.write_set.get(&myvar) == Some(&b"!".to_vec()));

    println!("\n  ══════════════════════════════════════════");
    println!("  FULL CHAIN TEST PASSED");
    println!("  ══════════════════════════════════════════");
}

#[tokio::test]
async fn test_choose_tx_among_conflicting_writers() {
    let client = Client::new(HOST);

    print_section!("CONFLICT — two writers compete for the same variable");
    println!("  When two Proposed transactions both depend on the same parent,");
    println!("  choose_tx() deterministically selects one winner (Approved)");
    println!("  and rejects the other (Rejected).");

    // ── Genesis ──
    println!("\n  → Setting up genesis with value \"42\"");
    let counter_var = unique_key("counter");
    let mut genesis = Transaction::new(&client);
    genesis.write_set.insert(counter_var.clone(), b"42".to_vec());
    genesis.read_set.insert(counter_var.clone(), Uuid::nil());
    genesis.commit().await.expect("genesis commit");
    genesis.approve().await.expect("genesis approve");
    print_transaction("genesis (Approved, value=\"42\")", &genesis);

    // ── Two writers ──
    println!("\n  → Creating two competing writers that both read the same variable");
    let mut writer_a = Transaction::new(&client);
    let mut writer_b = Transaction::new(&client);
    println!("  writer_a id: {}", writer_a.id);
    println!("  writer_b id: {}", writer_b.id);

    println!("\n  Both call read() — find_dependency resolves to genesis:");
    let val_a = writer_a.read(&counter_var).await.expect("writer_a read");
    let val_b = writer_b.read(&counter_var).await.expect("writer_b read");
    println!("  writer_a.read() = {:?}  (from genesis)", val_a.as_ref().map(|v| fmt_value(v)));
    println!("  writer_b.read() = {:?}  (from genesis)", val_b.as_ref().map(|v| fmt_value(v)));
    verify!("writer_a value is \"42\"", val_a.unwrap() == b"42".to_vec());
    verify!("writer_b value is \"42\"", val_b.unwrap() == b"42".to_vec());

    println!("\n  → Each writer writes a different proposed value:");
    println!("  writer_a writes \"43\"");
    println!("  writer_b writes \"44\"");
    writer_a.write(&counter_var, b"43".to_vec());
    writer_b.write(&counter_var, b"44".to_vec());

    println!("\n  → Both commit (status becomes Proposed):");
    let id_a = writer_a.id;
    let _id_b = writer_b.id;
    writer_a.commit().await.expect("writer_a commit");
    writer_b.commit().await.expect("writer_b commit");
    println!("  writer_a state: {:?}", writer_a.state);
    println!("  writer_b state: {:?}", writer_b.state);

    println!("\n  Variable '{}' has both tx ids registered:", counter_var);
    print_variable(&client, &counter_var).await;

    // ── Resolution ──
    println!("\n  → Calling Transaction::choose_tx([writer_a, writer_b]) ...");
    println!("     This uses a deterministic UUID-distance algorithm to pick a winner.");
    println!("     The winner becomes Approved, the loser becomes Rejected.");
    let mut writers = vec![writer_a, writer_b];
    let winner_id = Transaction::choose_tx(&mut writers).await.expect("choose_tx should succeed");
    let (winner, loser) = if winner_id == id_a { (0usize, 1usize) } else { (1usize, 0usize) };

    println!("\n  Result: winner = {} (idx {})", winner_id, winner);
    print_transaction("WINNER", &writers[winner]);
    print_transaction("LOSER", &writers[loser]);
    verify!("Winner state is Approved", matches!(writers[winner].state, TransactionStatus::Approved { .. }));
    verify!("Loser state is Rejected", matches!(writers[loser].state, TransactionStatus::Rejected { .. }));

    println!("\n  Variable '{}' after resolution:", counter_var);
    print_variable(&client, &counter_var).await;

    let var_ids = get_variable_ids(&client, &counter_var).await;
    verify!("Genesis removed from variable", !var_ids.contains(&genesis.id.to_string()));
    verify!("Winner present in variable", var_ids.contains(&winner_id.to_string()));
    verify!("Loser removed from variable", !var_ids.contains(&writers[loser].id.to_string()));

    let reconstructed = Transaction::from_uuid(winner_id, &client).await.expect("reconstruct winner");
    verify!("Reconstructed winner state is Approved", matches!(reconstructed.state, TransactionStatus::Approved { .. }));

    println!("\n  ══════════════════════════════════════════");
    println!("  CONFLICT RESOLUTION TEST PASSED: winner = {}", winner_id);
    println!("  ══════════════════════════════════════════");
}

#[tokio::test]
async fn test_read_nonexistent_variable_returns_none() {
    let client = Client::new(HOST);

    print_section!("EDGE CASE — reading a variable that doesn't exist");
    println!("  When no transaction has ever written to a variable,");
    println!("  find_dependency() finds no tx ids and returns None.");
    println!("  read() should return None without error.");

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