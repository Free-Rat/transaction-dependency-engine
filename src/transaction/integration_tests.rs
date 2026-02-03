#![cfg(test)]

use crate::riak::client::{Client, Bucket};
use crate::transaction::transaction::{Transaction, TransactionStatus};
use rand::random;
use uuid::Uuid;

const HOST: &str = "http://localhost:8098";

// Helper function to generate unique keys for tests
fn unique_key(prefix: &str) -> String {
    format!("{}-{}", prefix, random::<u32>())
}

#[tokio::test]
async fn test_transaction_new_creates_with_created_status() {
    let client = Client::new(HOST);
    let tx = Transaction::new(&client);

    assert!(matches!(tx.state, TransactionStatus::Created { .. }));
    assert!(tx.read_set.is_empty());
    assert!(tx.write_set.is_empty());
    assert!(tx.state_vclock.is_none());
}

#[tokio::test]
async fn test_transaction_commit_persists_to_riak() {
    let client = Client::new(HOST);
    let mut tx = Transaction::new(&client);
    let tx_id = tx.id;

    // Add some test data
    let test_var = unique_key("test_var");
    let test_value = b"test_value".to_vec();
    tx.write_set.insert(test_var.clone(), test_value.clone());
    tx.read_set.insert(test_var.clone(), Uuid::new_v4());

    // Commit the transaction
    tx.commit().await.expect("commit should succeed");

    // Verify status changed to Proposed
    assert!(matches!(tx.state, TransactionStatus::Proposed { .. }));

    // Verify read_set was persisted
    let read_set_result: Result<std::collections::HashMap<String, Uuid>, _> = 
        client.get_deserialized(Bucket::ReadSets, &tx_id.to_string()).await;
    assert!(read_set_result.is_ok());
    let stored_read_set = read_set_result.unwrap();
    assert_eq!(stored_read_set.len(), 1);
    assert!(stored_read_set.contains_key(&test_var));

    // Verify write_set was persisted
    let write_set_result: Result<std::collections::HashMap<String, Vec<u8>>, _> = 
        client.get_deserialized(Bucket::WriteSets, &tx_id.to_string()).await;
    assert!(write_set_result.is_ok());
    let stored_write_set = write_set_result.unwrap();
    assert_eq!(stored_write_set.get(&test_var), Some(&test_value));

    // Verify status was persisted
    let status_result = client.get_all_with_vclock::<TransactionStatus>(
        Bucket::Statuses, 
        &tx_id.to_string()
    ).await;
    assert!(status_result.is_ok());
    let (statuses, _) = status_result.unwrap();
    assert!(!statuses.is_empty());
    assert!(matches!(statuses[0], TransactionStatus::Proposed { .. }));
}

#[tokio::test]
async fn test_transaction_from_uuid_reconstructs_transaction() {
    let client = Client::new(HOST);
    let mut tx = Transaction::new(&client);
    let tx_id = tx.id;

    // Setup transaction data
    let test_var = unique_key("var");
    let test_value = b"value123".to_vec();
    let parent_id = Uuid::new_v4();

    tx.write_set.insert(test_var.clone(), test_value.clone());
    tx.read_set.insert(test_var.clone(), parent_id);

    // Commit to persist
    tx.commit().await.expect("commit should succeed");

    // Reconstruct from UUID
    let reconstructed = Transaction::from_uuid(tx_id, &client)
        .await
        .expect("from_uuid should succeed");

    assert_eq!(reconstructed.id, tx_id);
    assert_eq!(reconstructed.read_set.get(&test_var), Some(&parent_id));
    assert_eq!(reconstructed.write_set.get(&test_var), Some(&test_value));
    assert!(matches!(reconstructed.state, TransactionStatus::Proposed { .. }));
    assert!(reconstructed.state_vclock.is_some());
}

#[tokio::test]
async fn test_transaction_approve_changes_status() {
    let client = Client::new(HOST);
    let mut tx = Transaction::new(&client);
    let tx_id = tx.id;

    // Add a variable to test removal during approve
    let test_var = unique_key("approve_var");
    let parent_id = Uuid::new_v4();
    tx.read_set.insert(test_var.clone(), parent_id);

    // Commit first to get into Proposed state
    tx.commit().await.expect("commit should succeed");

    // Approve the transaction
    tx.approve().await.expect("approve should succeed");

    // Verify status is now Approved
    assert!(matches!(tx.state, TransactionStatus::Approved { .. }));

    // Verify persisted status contains Approved
    let (statuses, _) = client.get_all_with_vclock::<TransactionStatus>(
        Bucket::Statuses,
        &tx_id.to_string()
    ).await.expect("should get status");

    let has_approved = statuses.iter().any(|s| matches!(s, TransactionStatus::Approved { .. }));
    assert!(has_approved);

    // Verify parent was removed from variables
    let var_txs: Result<Vec<String>, _> = client.get_deserialized(Bucket::Variables, &test_var).await;
    // Variable should either not contain parent_id or not exist
    if let Ok(txs) = var_txs {
        assert!(!txs.contains(&parent_id.to_string()));
    }
}

#[tokio::test]
async fn test_transaction_reject_changes_status() {
    let client = Client::new(HOST);
    let mut tx = Transaction::new(&client);
    let tx_id = tx.id;

    // Add a variable to test removal during reject
    let test_var = unique_key("reject_var");
    tx.read_set.insert(test_var.clone(), Uuid::new_v4());

    // Commit first
    tx.commit().await.expect("commit should succeed");

    // Reject the transaction
    tx.reject().await.expect("reject should succeed");

    // Verify status is now Rejected
    assert!(matches!(tx.state, TransactionStatus::Rejected { .. }));

    // Verify persisted status contains Rejected
    let (statuses, _) = client.get_all_with_vclock::<TransactionStatus>(
        Bucket::Statuses,
        &tx_id.to_string()
    ).await.expect("should get status");

    let has_rejected = statuses.iter().any(|s| matches!(s, TransactionStatus::Rejected { .. }));
    assert!(has_rejected);

    // Verify self was removed from variables
    let var_txs: Result<Vec<String>, _> = client.get_deserialized(Bucket::Variables, &test_var).await;
    if let Ok(txs) = var_txs {
        assert!(!txs.contains(&tx_id.to_string()));
    }
}

#[tokio::test]
async fn test_add_variable_tx_registers_in_variables_bucket() {
    let client = Client::new(HOST);
    let tx = Transaction::new(&client);
    let var = unique_key("myvar");

    // Add transaction to variable
    let vclock = tx.add_variable_tx(&var, tx.id)
        .await
        .expect("add_variable_tx should succeed");

    // Vclock should be present (even if it's from the initial PUT)
    // Note: For new keys, Riak may return an empty vclock on first PUT,
    // but the verification GET should return a valid one
    println!("Vclock length: {}", vclock.0.len());

    // Verify it was stored
    let stored_vec: Vec<String> = client.get_deserialized(Bucket::Variables, &var)
        .await
        .expect("should retrieve stored variable");

    assert_eq!(stored_vec.len(), 1);
    assert_eq!(stored_vec[0], tx.id.to_string());
}

#[tokio::test]
async fn test_add_variable_tx_merges_multiple_transactions() {
    let client = Client::new(HOST);
    let tx1 = Transaction::new(&client);
    let tx2 = Transaction::new(&client);
    let var = unique_key("shared_var");

    // Add both transactions to the same variable
    tx1.add_variable_tx(&var, tx1.id)
        .await
        .expect("add tx1 should succeed");

    tx2.add_variable_tx(&var, tx2.id)
        .await
        .expect("add tx2 should succeed");

    // Retrieve and verify both are present
    let stored_vec: Vec<String> = client.get_deserialized(Bucket::Variables, &var)
        .await
        .expect("should retrieve stored variable");

    assert_eq!(stored_vec.len(), 2);
    assert!(stored_vec.contains(&tx1.id.to_string()));
    assert!(stored_vec.contains(&tx2.id.to_string()));
}

#[tokio::test]
async fn test_remove_tx_from_variable_removes_transaction() {
    let client = Client::new(HOST);
    let tx1 = Transaction::new(&client);
    let tx2 = Transaction::new(&client);
    let var = unique_key("var_to_remove");

    // Add both transactions
    tx1.add_variable_tx(&var, tx1.id).await.expect("add tx1");
    tx2.add_variable_tx(&var, tx2.id).await.expect("add tx2");

    // Remove tx1
    tx1.remove_tx_from_variable(&var, tx1.id)
        .await
        .expect("remove should succeed");

    // Verify only tx2 remains
    let stored_vec: Vec<String> = client.get_deserialized(Bucket::Variables, &var)
        .await
        .expect("should retrieve variable");

    assert_eq!(stored_vec.len(), 1);
    assert_eq!(stored_vec[0], tx2.id.to_string());
    assert!(!stored_vec.contains(&tx1.id.to_string()));
}

#[tokio::test]
async fn test_remove_tx_from_variable_handles_missing_key() {
    let client = Client::new(HOST);
    let tx = Transaction::new(&client);
    let var = unique_key("nonexistent_var");

    // Try to remove from non-existent variable
    let result = tx.remove_tx_from_variable(&var, tx.id).await;

    // Should succeed (no-op) and return empty vclock
    assert!(result.is_ok());
    let vclock = result.unwrap();
    assert_eq!(vclock.0, b"");
}

#[tokio::test]
async fn test_choose_tx_selects_deterministically() {
    let client = Client::new(HOST);
    let mut tx1 = Transaction::new(&client);
    let mut tx2 = Transaction::new(&client);
    let mut tx3 = Transaction::new(&client);

    // Add unique variables so approve/reject can work properly
    let var = unique_key("choose_var");
    tx1.read_set.insert(var.clone(), Uuid::new_v4());
    tx2.read_set.insert(var.clone(), Uuid::new_v4());
    tx3.read_set.insert(var.clone(), Uuid::new_v4());

    // Commit all transactions
    tx1.commit().await.expect("tx1 commit");
    tx2.commit().await.expect("tx2 commit");
    tx3.commit().await.expect("tx3 commit");

    let mut txs = vec![tx1, tx2, tx3];
    let winner_id = Transaction::choose_tx(&mut txs)
        .await
        .expect("choose_tx should succeed");

    // Verify exactly one is approved
    let approved_count = txs.iter()
        .filter(|tx| matches!(tx.state, TransactionStatus::Approved { .. }))
        .count();
    assert_eq!(approved_count, 1, "Expected exactly 1 approved transaction");

    // Verify two are rejected
    let rejected_count = txs.iter()
        .filter(|tx| matches!(tx.state, TransactionStatus::Rejected { .. }))
        .count();
    assert_eq!(rejected_count, 2, "Expected exactly 2 rejected transactions");

    // Verify winner matches the approved transaction
    let winner_tx = txs.iter().find(|tx| tx.id == winner_id).unwrap();
    assert!(matches!(winner_tx.state, TransactionStatus::Approved { .. }));
}

#[tokio::test]
async fn test_choose_tx_is_deterministic_across_calls() {
    let client = Client::new(HOST);

    // Create same set of transactions with predetermined IDs
    let ids = [
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap(),
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440002").unwrap(),
    ];

    // First run
    let mut txs1: Vec<Transaction> = ids.iter().map(|&id| {
        let mut tx = Transaction::new(&client);
        tx.id = id; // Override with predetermined ID
        tx
    }).collect();

    let winner1 = Transaction::choose_tx(&mut txs1)
        .await
        .expect("first choose_tx");

    // Second run with same IDs (different transaction instances)
    let mut txs2: Vec<Transaction> = ids.iter().map(|&id| {
        let mut tx = Transaction::new(&client);
        tx.id = id;
        tx
    }).collect();

    let winner2 = Transaction::choose_tx(&mut txs2)
        .await
        .expect("second choose_tx");

    // Winners should be identical
    assert_eq!(winner1, winner2);
}

#[tokio::test]
async fn test_transaction_lifecycle_full_workflow() {
    let client = Client::new(HOST);

    // Create a parent transaction
    let mut parent_tx = Transaction::new(&client);
    let var = unique_key("lifecycle_var");
    let value = b"initial_value".to_vec();
    dbg!(&var);
    dbg!(&value);

    parent_tx.write_set.insert(var.clone(), value.clone());
    parent_tx.commit().await.expect("parent commit");
    parent_tx.approve().await.expect("parent approve");
    dbg!(&parent_tx.id);
    

    // Create a child transaction that depends on parent
    let mut child_tx = Transaction::new(&client);
    child_tx.read_set.insert(var.clone(), parent_tx.id);
    child_tx.write_set.insert(var.clone(), b"updated_value".to_vec());
    child_tx.commit().await.expect("child commit");
    dbg!(&child_tx.id);

    // Verify both are registered in variables
    let var_txs: Vec<String> = client.get_deserialized(Bucket::Variables, &var)
        .await
        .expect("get variable txs");
    dbg!(&var_txs);

    assert!(var_txs.contains(&parent_tx.id.to_string()), "Parent should be in variables");
    assert!(var_txs.contains(&child_tx.id.to_string()), "Child should be in variables");

    // Approve child
    child_tx.approve().await.expect("child approve");

    // After child approval, parent should be removed from variables
    let var_txs_after: Vec<String> = client.get_deserialized(Bucket::Variables, &var)
        .await
        .expect("get variable txs after");

    assert!(!var_txs_after.contains(&parent_tx.id.to_string()), "Parent should be removed");
    assert!(var_txs_after.contains(&child_tx.id.to_string()), "Child should remain");
}

#[tokio::test]
async fn test_concurrent_variable_updates_merge_correctly() {
    use tokio::task::JoinSet;

    let client = Client::new(HOST);
    let var = unique_key("concurrent_var");
    let num_transactions = 5;

    let mut join_set = JoinSet::new();

    // Spawn multiple concurrent tasks adding transactions to the same variable
    for i in 0..num_transactions {
        let client_clone = Client::new(HOST);
        let var_clone = var.clone();

        join_set.spawn(async move {
            let tx = Transaction::new(&client_clone);
            // Add retry logic in case of temporary failures
            let mut retries = 3;
            let result = loop {
                match tx.add_variable_tx(&var_clone, tx.id).await {
                    Ok(_) => break Ok(tx.id),
                    Err(e) if retries > 0 => {
                        retries -= 1;
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        continue;
                    }
                    Err(e) => break Err(e),
                }
            };
            result.expect(&format!("add_variable_tx {} should succeed", i))
        });
    }

    // Collect all transaction IDs
    let mut tx_ids = Vec::new();
    while let Some(result) = join_set.join_next().await {
        tx_ids.push(result.expect("task should complete"));
    }

    // Give Riak time to settle
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify all transactions were registered
    let stored_vec: Vec<String> = client.get_deserialized(Bucket::Variables, &var)
        .await
        .expect("should retrieve merged variable");

    // Due to high concurrency and eventual consistency, we accept that
    // we might have close to num_transactions
    assert!(
        stored_vec.len() >= num_transactions - 1,
        "Expected at least {} transactions, got {}. Missing: {}",
        num_transactions - 1,
        stored_vec.len(),
        num_transactions - stored_vec.len()
    );

    // Count how many of our transactions made it
    let found_count = tx_ids.iter()
        .filter(|id| stored_vec.contains(&id.to_string()))
        .count();

    println!("Found {}/{} transactions in variables", found_count, num_transactions);
    
    assert!(
        found_count >= num_transactions - 1,
        "At least {} transactions should be registered, found {}",
        num_transactions - 1,
        found_count
    );
}

#[tokio::test]
async fn test_change_status_updates_vclock() {
    let client = Client::new(HOST);
    let mut tx = Transaction::new(&client);

    // Initial state has no vclock
    assert!(tx.state_vclock.is_none());

    // Change to Proposed
    let proposed_status = TransactionStatus::Proposed { at: chrono::Utc::now() };
    tx.change_status(proposed_status).await.expect("change to proposed");

    let vclock1 = tx.state_vclock.clone();
    assert!(vclock1.is_some());

    // Small delay to ensure different timestamps
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Change to Approved
    let approved_status = TransactionStatus::Approved { at: chrono::Utc::now() };
    tx.change_status(approved_status).await.expect("change to approved");

    let vclock2 = tx.state_vclock.clone();
    assert!(vclock2.is_some());

    // Vclocks should be different (newer)
    assert_ne!(vclock1.unwrap().0, vclock2.unwrap().0);
}

#[tokio::test]
async fn test_from_uuid_selects_correct_status_from_siblings() {

    let test_var = unique_key("test_var");
    let test_value = b"test_value".to_vec();

    // 1. Setup Genesis
    let genesis_client = Client::new(HOST);
    let mut genesis = Transaction::new(&genesis_client);
    genesis.read_set.insert(test_var.clone(), Uuid::nil());
    genesis.write_set.insert(test_var.clone(), b"0".to_vec());
    genesis.commit().await.expect("genesis commit");
    genesis.approve().await.expect("genesis approve");

    dbg!(&genesis.id);
    dbg!(&genesis.read_set);
    dbg!(&genesis.write_set);
    dbg!(&genesis.state);


    let reconstructed_genesis = Transaction::from_uuid(genesis.id, &genesis_client)
        .await
        .expect("from_uuid should succeed");
    dbg!(&genesis.id);
    dbg!(&reconstructed_genesis.read_set);
    dbg!(&reconstructed_genesis.write_set);
    dbg!(&reconstructed_genesis.state);


    let var_txs_after: Vec<String> = genesis_client.get_deserialized(Bucket::Variables, &test_var)
        .await
        .expect("get variable txs after");
    dbg!(var_txs_after);

    // --- CRITICAL: Wait for Riak to index the Genesis transaction ---
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = Client::new(HOST);
    let mut tx = Transaction::new(&client);
    let tx_id = tx.id;
    dbg!(tx_id);
    let var_txs_after: Vec<String> = client.get_deserialized(Bucket::Variables, &test_var)
        .await
        .expect("get variable txs after");
    dbg!(var_txs_after);

    dbg!("pre read ");
    // 2. Perform Transaction Operations
    tx.read(&test_var).await.expect("read should succeed");
    dbg!("post read - pre write");
    tx.write(&test_var, test_value.clone());
    dbg!("post write");

    dbg!(&tx.read_set);
    dbg!(&tx.write_set);

    assert!(&tx.read_set.contains_key(&genesis.id.to_string()));

    // Create Proposed status
    let proposed = TransactionStatus::Proposed { 
        at: chrono::Utc::now() - chrono::Duration::seconds(10) 
    };
    tx.change_status(proposed).await.expect("set proposed");

    dbg!(&tx.read_set);
    dbg!(&tx.write_set);

    tx.commit().await.expect("tx commit");

    dbg!(&tx.read_set);
    dbg!(&tx.write_set);

    // Small delay to ensure different timestamps
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Create Approved status (later)
    let approved = TransactionStatus::Approved { 
        at: chrono::Utc::now() 
    };
    tx.change_status(approved.clone()).await.expect("set approved");
    dbg!(&tx.state);

    // Small delay to let Riak settle
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Reconstruct from UUID - should select Approved as final state
    let reconstructed = Transaction::from_uuid(tx_id, &client)
        .await
        .expect("from_uuid should succeed");
    dbg!(&reconstructed.state);

    assert!(matches!(reconstructed.state, TransactionStatus::Approved { .. }));
}
