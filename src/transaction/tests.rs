#![cfg(test)]

use httptest::{Expectation, Server};
use httptest::matchers::{all_of, request};
use httptest::responders;
use uuid::Uuid;

use crate::riak::object::VClock;
use crate::riak::client::{Client, Bucket};
use crate::transaction::transaction::{Transaction, TransactionStatus};


#[tokio::test]
async fn test_add_variable_tx_registers_tx_in_variables() {
    let server = Server::run();
    let client = Client::new(server.url("/").to_string());

    let tx = Transaction::new(&client);
    let var = "some_var";

    // Expect GET -> 404
    server.expect(
        Expectation::matching(all_of![
            request::method("GET"),
            request::path(format!(
                "/types/default/buckets/{}/keys/{}",
                Bucket::Variables,
                var
            ))
        ])
            .times(..) // match 0 or more times
            .respond_with(responders::status_code(404)),
    );

    // Expect PUT -> return vclock
    let returned_vclock = VClock(b"vars-vc-1".to_vec());
    server.expect(
        Expectation::matching(all_of![
            request::method("PUT"),
            request::path(format!(
                "/types/default/buckets/{}/keys/{}",
                Bucket::Variables,
                var
            ))
        ])
            .respond_with(
                responders::status_code(204)
                    .append_header("X-Riak-Vclock", returned_vclock.to_base64()),
            ),
    );

    // Expect GET -> return stored vector serialized
    let stored_vec = vec![tx.id.to_string()];
    let stored_bytes = postcard::to_stdvec(&stored_vec).expect("serialize should succeed");

    server.expect(
        Expectation::matching(all_of![
            request::method("GET"),
            request::path(format!(
                "/types/default/buckets/{}/keys/{}",
                Bucket::Variables,
                var
            ))
        ])
            .times(..)
            .respond_with(
                responders::status_code(200)
                    .append_header("Content-Type", "application/octet-stream")
                    .append_header("X-Riak-Vclock", returned_vclock.to_base64())
                    .body(stored_bytes.clone()),
            ),
    );

    let vc = tx.add_variable_tx(var, tx.id).await.expect("add_variable_tx failed");
    assert_eq!(vc, returned_vclock);
}

#[tokio::test]
async fn test_commit_registers_and_proposes() {
    let server = Server::run();
    let client = Client::new(server.url("/").to_string());

    let mut tx = Transaction::new(&client);
    tx.read_set.insert("counter".to_string(), Uuid::new_v4());
    tx.write_set.insert("payload".to_string(), b"hello".to_vec());

    // Prepare vclocks
    let v_rs = VClock(b"vc-readsets".to_vec());
    let v_ws = VClock(b"vc-writesets".to_vec());
    let v_status = VClock(b"vc-statuses".to_vec());
    let v_vars = VClock(b"vc-variables".to_vec());

    // 1) PUT ReadSets
    server.expect(
        Expectation::matching(all_of![
            request::method("PUT"),
            request::path(format!(
                "/types/default/buckets/{}/keys/{}",
                Bucket::ReadSets,
                tx.id
            ))
        ])
            .times(..)
            .respond_with(
                responders::status_code(204)
                    .append_header("X-Riak-Vclock", v_rs.to_base64()),
            ),
    );

    // 2) PUT WriteSets
    server.expect(
        Expectation::matching(all_of![
            request::method("PUT"),
            request::path(format!(
                "/types/default/buckets/{}/keys/{}",
                Bucket::WriteSets,
                tx.id
            ))
        ])
            .times(..)
            .respond_with(
                responders::status_code(204)
                    .append_header("X-Riak-Vclock", v_ws.to_base64()),
            ),
    );

    // 3) PUT Statuses
    server.expect(
        Expectation::matching(all_of![
            request::method("PUT"),
            request::path(format!(
                "/types/default/buckets/{}/keys/{}",
                Bucket::Statuses,
                tx.id
            ))
        ])
            .times(..)
            .respond_with(
                responders::status_code(204)
                    .append_header("X-Riak-Vclock", v_status.to_base64()),
            ),
    );

    // 4) GET initial variable -> 404
    let var = "counter";
    server.expect(
        Expectation::matching(all_of![
            request::method("GET"),
            request::path(format!(
                "/types/default/buckets/{}/keys/{}",
                Bucket::Variables,
                var
            ))
        ])
            .times(..)
            .respond_with(responders::status_code(404)),
    );

    // 5) PUT merged variable
    server.expect(
        Expectation::matching(all_of![
            request::method("PUT"),
            request::path(format!(
                "/types/default/buckets/{}/keys/{}",
                Bucket::Variables,
                var
            ))
        ])
            .times(..)
            .respond_with(
                responders::status_code(204)
                    .append_header("X-Riak-Vclock", v_vars.to_base64()),
            ),
    );

    // 6) GET merged variable
    let stored_vec = vec![tx.id.to_string()];
    let stored_bytes = postcard::to_stdvec(&stored_vec).unwrap();
    server.expect(
        Expectation::matching(all_of![
            request::method("GET"),
            request::path(format!(
                "/types/default/buckets/{}/keys/{}",
                Bucket::Variables,
                var
            ))
        ])
            .times(..)
            .respond_with(
                responders::status_code(200)
                    .append_header("Content-Type", "application/octet-stream")
                    .append_header("X-Riak-Vclock", v_vars.to_base64())
                    .body(stored_bytes.clone()),
            ),
    );

    // Commit transaction
    tx.commit().await.expect("commit failed");

    // Validate state
    match tx.state {
        TransactionStatus::Proposed { .. } => {}
        _ => panic!("expected Proposed state after commit"),
    }
    assert_eq!(tx.state_vclock, Some(v_status));
}
