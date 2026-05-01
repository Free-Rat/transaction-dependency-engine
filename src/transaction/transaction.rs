use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use crate::riak::client::Client;
use crate::riak::client::Bucket;
use crate::riak::client::RiakError;
use crate::riak::object::VClock;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransactionStatus {
    Created   { at: DateTime<Utc> },
    // Prepared  { at: DateTime<Utc> },
    Proposed  { at: DateTime<Utc> },
    Approved  { at: DateTime<Utc> },
    // Committed { at: DateTime<Utc> },
    Rejected  { at: DateTime<Utc> },
}

impl TransactionStatus {
    fn at(&self) -> DateTime<Utc> {
        match self {
            TransactionStatus::Created { at }
            // | TransactionStatus::Prepared { at }
            | TransactionStatus::Proposed { at }
            | TransactionStatus::Approved { at }
            | TransactionStatus::Rejected { at } => *at,
        }
    }

    fn is_final(&self) -> bool {
        matches!(self, TransactionStatus::Approved { .. } | TransactionStatus::Rejected { .. })
    }
}

#[derive(Debug)]
pub struct Transaction<'a> {
    pub id: Uuid,
    pub read_set: HashMap<String, Uuid>, // dependecy set
    pub write_set: HashMap<String, Vec<u8>>,
    pub state: TransactionStatus,
    pub state_vclock: Option<VClock>,
    connection: &'a Client,
}

impl<'a> Transaction<'a> {
    pub fn new(connection: &'a Client) -> Self {
        let id = Uuid::new_v4();
        let read_set: HashMap<String,Uuid> = HashMap::new();
        let write_set: HashMap<String,Vec<u8>> = HashMap::new();
        let state: TransactionStatus = TransactionStatus::Created{at: Utc::now()};
        // let client = conn;

        Transaction {
            id,
            read_set,
            write_set,
            state,
            state_vclock: None,
            connection,
        }
    }

    pub async fn from_uuid(id: Uuid, connection: &'a Client) -> Result<Self, RiakError> {
        // TODO: 
        // let read_set: HashMap<String,Uuid> = connection.get(Bucket::ReadSets, &id.to_string()).await; 
        // let write_set: HashMap<String,Vec<u8>> = connection.get(Bucket::WriteSets, &id.to_string()).await;
        // let state: TransactionStatus = connection.get(Bucket::Statuses, &id.to_string()).await;
        let read_set: HashMap<String, Uuid> = connection.get_deserialized(Bucket::ReadSets, &id.to_string()).await?;
        let write_set: HashMap<String, Vec<u8>> = connection.get_deserialized(Bucket::WriteSets, &id.to_string()).await?;


        // -> use new helper to get all status siblings AND the vclock
        let (possible_states, status_vclock) = connection.get_all_with_vclock::<TransactionStatus>(Bucket::Statuses, &id.to_string()).await?;
        let state: TransactionStatus = {
            // wybieramy pierwszy Rejected lub Approved (earliest final)
            if let Some(final_state) = possible_states
                .iter()
                .filter(|s| s.is_final())
                .min_by_key(|s| s.at())
            {
                final_state.clone()
            } else {
                // jeśli nie ma to wybieramy najpóźniejszy status
                possible_states
                    .into_iter()
                    .max_by_key(|s| s.at())
                    .unwrap_or(TransactionStatus::Created {
                        at: DateTime::<Utc>::MIN_UTC,
                    })
            }
        };

        Ok(Self {
            id,
            read_set,
            write_set,
            state,
            state_vclock: Some(status_vclock),
            connection
        })
    }

    pub fn write(&mut self, key: &str, value: Vec<u8> ) {
        // save to write_set
        let record = self.write_set.entry(key.into()).or_default();
        *record = value;

        // save to read_set - for ony write transactions
        // to jednak nie jest potrzebne, jeśli nie ma readów do zmiennej
        // to znaczy ze na niej po prostu nie polega mi możemy to zwalidować just fine
        //
        // let record = self.read_set.entry(key.into()).or_default();
        // *record = value;

        // riak client put is later in commit
    }


async fn find_dependency(&self, key: &str) -> Result<Option<Uuid>, RiakError> {
        // Get all tx ids registered for this variable from the Variables bucket
        let possible_dependencies_ids: Vec<String> = match self.connection
            .get_all_with_vclock::<Vec<String>>(Bucket::Variables, key)
            .await
        {
            Ok((vecs, _vc)) => {
                let mut ids: Vec<String> = vecs.into_iter().flatten().collect();
                ids.retain(|s| s != "$");
                ids
            }
            Err(RiakError::UnexpectedStatus(404)) => return Ok(None),
            Err(e) => return Err(e),
        };

        if possible_dependencies_ids.is_empty() {
            return Ok(None);
        }

        // For each dependency, get its read_set to find parents and their variables
        let mut parents: HashSet<Uuid> = HashSet::new();
        let mut variables: HashSet<String> = HashSet::new();

        for pd_id in &possible_dependencies_ids {
            if pd_id == &Uuid::nil().to_string() {
                continue;
            }
            let read_set_result: HashMap<String, Uuid> = match self.connection
                .get_deserialized(Bucket::ReadSets, pd_id)
                .await
            {
                Ok(rs) => rs,
                Err(RiakError::UnexpectedStatus(404)) => continue,
                Err(e) => return Err(e),
            };
            parents.extend(read_set_result.values().cloned());
            variables.extend(read_set_result.keys().cloned());
        }

        // Collect ALL tx ids: direct dependencies + transitive ones from parent variables
        let mut all_tx_ids: HashSet<String> = possible_dependencies_ids.into_iter().collect();
        all_tx_ids.remove(&Uuid::nil().to_string());

        for var in &variables {
            match self.connection
                .get_all_with_vclock::<Vec<String>>(Bucket::Variables, var)
                .await
            {
                Ok((sibling_vecs, _)) => {
                    for sibling in sibling_vecs {
                        for id in sibling {
                            if id != "$" {
                                all_tx_ids.insert(id);
                            }
                        }
                    }
                }
                Err(RiakError::UnexpectedStatus(404)) => {}
                Err(e) => return Err(e),
            }
        }

        // Load all transactions
        let tx_futures = all_tx_ids.into_iter().map(|id| {
            let uuid = Uuid::try_parse(&id).map_err(|_| RiakError::InvalidId);
            async move {
                Transaction::from_uuid(uuid?, self.connection).await
            }
        });
        let txs: Vec<Transaction> = futures::future::try_join_all(tx_futures).await?;

        // Find the latest approved parent for each variable
        let latest_parents: HashMap<String, Uuid> = {
            let mut latest: HashMap<String, (Uuid, DateTime<Utc>)> = HashMap::new();

            for tx in &txs {
                if !matches!(tx.state, TransactionStatus::Approved { .. }) {
                    continue;
                }

                let at = tx.state.at();

                for (var, parent_id) in &tx.read_set {
                    if parent_id == &Uuid::nil() {
                        continue;
                    }
                    match latest.get(var) {
                        Some((_, existing_at)) if *existing_at >= at => {}
                        _ => {
                            latest.insert(var.clone(), (*parent_id, at));
                        }
                    }
                }
            }

            latest
                .into_iter()
                .map(|(var, (parent_id, _))| (var, parent_id))
                .collect()
        };

        // Filter and select candidates
        let mut potential_dependencies: Vec<Transaction> = Vec::new();

        'txs: for mut tx in txs {
            // Skip already-rejected transactions
            if matches!(tx.state, TransactionStatus::Rejected { .. }) {
                continue;
            }

            // Reject if tx doesn't involve this variable at all
            if !tx.read_set.contains_key(key) && !tx.write_set.contains_key(key) {
                tx.reject().await?;
                continue 'txs;
            }

            // Check if tx is on the frontline for this key
            match latest_parents.get(key) {
                Some(latest_parent) => {
                    match tx.read_set.get(key) {
                        Some(tx_parent) if tx_parent == latest_parent => {
                            // on the frontline
                        }
                        Some(_) => {
                            // not on frontline for this key
                            if !matches!(tx.state, TransactionStatus::Approved { .. }) {
                                tx.reject().await?;
                            }
                            continue 'txs;
                        }
                        None => {
                            // tx writes to key but doesn't read from it (genesis-like)
                            // — skip frontline check, it's a valid candidate
                        }
                    }
                }
                None => {
                    // No approved parent exists for this variable.
                    // This means this tx is either genesis or the first writer.
                    // It's a valid candidate — don't reject.
                }
            }

            potential_dependencies.push(tx);
        }

        // If no candidates, return the latest approved parent for this key (if any)
        if potential_dependencies.is_empty() {
            let Some(&winner) = latest_parents.get(key) else {
                return Ok(None);
            };
            return Ok(Some(winner));
        }

        // If only one candidate, approve it and return
        if potential_dependencies.len() == 1 {
            let mut winner = potential_dependencies.into_iter().next().unwrap();
            if !matches!(winner.state, TransactionStatus::Approved { .. }) {
                winner.approve().await?;
            }
            return Ok(Some(winner.id));
        }

        // Prefer the earliest Approved candidate
        if let Some(winner) = potential_dependencies
            .iter()
            .filter(|tx| matches!(tx.state, TransactionStatus::Approved { .. }))
            .min_by_key(|tx| tx.state.at())
        {
            return Ok(Some(winner.id));
        }

        // Otherwise, deterministically choose among Proposed candidates
        let winner = Transaction::choose_tx(&mut potential_dependencies).await?;
        Ok(Some(winner))
    }

    // let GetResult::Siblings(possible_dependencies) = possible_dependencies else {
    //     return Some(Uuid::parse_str(possible_dependencies.into()).expect("Invalid UUID"));
    // };

    pub async fn read(&mut self, key: &str) -> Result<Option<Vec<u8>>, RiakError> {
        if let Some(p_id) = self.find_dependency(key).await? {
            let p_tx = Transaction::from_uuid(p_id, self.connection).await?;
            self.read_set.insert(key.to_string(), p_id);
            Ok(p_tx.write_set.get(key).cloned())
        } else {
            Ok(None)
        }
    }

    pub async fn approve(&mut self) -> Result<(), RiakError> {
        let new_state = TransactionStatus::Approved { at: Utc::now() };
        self.change_status(new_state).await?;
        // remove my parent from variables for all my readed key
        for (var, parent_id) in &self.read_set {
            self.remove_tx_from_variable(var, *parent_id).await?;
        }
        Ok(())
    }

    pub async fn reject(&mut self) -> Result<(), RiakError> {
        let new_state = TransactionStatus::Rejected { at: Utc::now() };
        self.change_status(new_state).await?;
        // remove my self from variables for all my readed variables
        for var in self.read_set.keys() {
            self.remove_tx_from_variable(var, self.id).await?;
        }
        Ok(())
    }


    pub async fn commit(&mut self) -> Result<(), RiakError> {
        // Persist read_set (unique key per tx.id — no vclock needed)
        let read_bytes = postcard::to_stdvec(&self.read_set)
            .map_err(|e| RiakError::Serialize(e.to_string()))?;
        let _vc_rs = self
            .connection
            .put(Bucket::ReadSets, &self.id.to_string(), read_bytes, None)
        .await?;

        // Persist write_set (unique key per tx.id — no vclock needed)
        let write_bytes = postcard::to_stdvec(&self.write_set)
            .map_err(|e| RiakError::Serialize(e.to_string()))?;
        let _vc_ws = self
            .connection
            .put(Bucket::WriteSets, &self.id.to_string(), write_bytes, None)
        .await?;

        // Advance status to Proposed (or Committed if you add that variant)
        let new_status = TransactionStatus::Proposed { at: Utc::now() };
        self.change_status(new_status).await?;

        // Register this tx.id in Variables for every variable in read_set.
        //   Use your add_variable_tx merge helper to avoid siblings.
        //   We iterate over keys by reference to avoid moving the map.
        for var in self.write_set.keys() {
            // add_variable_tx returns the vclock of the Variables object after the merge;
            // we ignore it here, but you could store or log it if desired.
            self.add_variable_tx(var, self.id).await?;
        }

        Ok(())
    }

    /// Add a single tx id to the Variables bucket for `var`.
    /// Ensures deterministic serialization (sorted Vec) and retries merge if races happen.
    // 2. Fix add_variable_tx to handle empty vclock properly and improve retry logic
    pub async fn add_variable_tx(&self, var: &str, tx_id: Uuid) -> Result<VClock, RiakError> {
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            // GET siblings (each sibling deserializes to Vec<String>)
            let (sibling_vecs, maybe_vclock): (Vec<Vec<String>>, Option<VClock>) = match 
            self.connection.get_all_with_vclock::<Vec<String>>(Bucket::Variables, var).await
            {
                Ok((vecs, vc)) => (vecs, Some(vc)),
                // treat 404 as "no object yet"
                Err(RiakError::UnexpectedStatus(404)) => (Vec::new(), None),
                Err(e) => return Err(e),
            };

            // Flatten siblings into a single set (union of all siblings)
            let mut set: HashSet<String> = HashSet::new();
            for sibling in sibling_vecs {
                for id in sibling {
                    set.insert(id);
                }
            }

            // Insert our tx id
            set.insert(tx_id.to_string());

            // Deterministic ordering: sorted Vec
            let mut out_vec: Vec<String> = set.into_iter().collect();
            out_vec.sort();

            // Serialize deterministically
            let bytes = postcard::to_stdvec(&out_vec)
                .map_err(|e| RiakError::Serialize(e.to_string()))?;

            // PUT with previous vclock if present
            let _new_vclock = self.connection.put(
                Bucket::Variables,
                var,
                bytes.clone(),
                maybe_vclock,
            ).await?;

            // Add exponential backoff before verification to reduce contention
            if attempt > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(10 * (1 << attempt))).await;
            }

            // Verify stored value matches our merged set (flatten stored siblings and compare sets)
            let (stored_sibling_vecs, stored_vclock) =
            match self.connection.get_all_with_vclock::<Vec<String>>(Bucket::Variables, var).await {
                Ok((vecs, vc)) => (vecs, vc),
                Err(e) => {
                    // If verification GET fails, retry
                    if attempt < MAX_RETRIES - 1 {
                        continue;
                    }
                    return Err(e);
                }
            };

            let mut stored_set: HashSet<String> = HashSet::new();
            for sibling in stored_sibling_vecs {
                for id in sibling {
                    stored_set.insert(id);
                }
            }

            // Check if our tx_id is present (success condition)
            if stored_set.contains(&tx_id.to_string()) {
                // Return the authoritative vclock from verification GET
                return Ok(stored_vclock);
            }

            // else: race happened — retry (loop will GET new state)
        }

        // After all retries, do final verification
        let (final_sibling_vecs, final_vclock) =
        self.connection.get_all_with_vclock::<Vec<String>>(Bucket::Variables, var).await?;
        let mut final_set = HashSet::new();
        for sibling in final_sibling_vecs {
            for id in sibling {
                final_set.insert(id);
            }
        }
        if final_set.contains(&tx_id.to_string()) {
            Ok(final_vclock)
        } else {
            Err(RiakError::UnexpectedStatus(409)) // or a custom Conflict error
        }
    }

    // 3. Similar fix for remove_tx_from_variable
    pub async fn remove_tx_from_variable(&self, var: &str, tx_id: Uuid) -> Result<VClock, RiakError> {
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            // 1) GET siblings + vclock
            let (sibling_vecs, maybe_vclock): (Vec<Vec<String>>, Option<VClock>) = match 
            self.connection.get_all_with_vclock::<Vec<String>>(Bucket::Variables, var).await
            {
                Ok((vecs, vc)) => (vecs, Some(vc)),
                // missing key → nothing to remove
                Err(RiakError::UnexpectedStatus(404)) => return Ok(VClock(b"".to_vec())),
                Err(e) => return Err(e),
            };

            // 2) Flatten siblings into a set
            let mut set: HashSet<String> = HashSet::new();
            for sibling in sibling_vecs {
                for id in sibling {
                    set.insert(id);
                }
            }

            // 3) If id not present, no-op and return current vclock
            let tx_id_s = tx_id.to_string();
            if !set.contains(&tx_id_s) {
                // nothing to remove
                if let Some(vc) = maybe_vclock {
                    return Ok(vc);
                } else {
                    return Ok(VClock(b"".to_vec()));
                }
            }

            // 4) Remove the id
            set.remove(&tx_id_s);

            // 5) Deterministic ordering: sorted Vec
            let mut out_vec: Vec<String> = set.into_iter().collect();
            out_vec.sort();

            // 6) Serialize deterministically
            let bytes = postcard::to_stdvec(&out_vec)
                .map_err(|e| RiakError::Serialize(e.to_string()))?;

            // 7) PUT using previous vclock if present
            let _new_vclock = self.connection.put(
                Bucket::Variables,
                var,
                bytes.clone(),
                maybe_vclock,
            ).await?;

            // Add backoff before verification
            if attempt > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(10 * (1 << attempt))).await;
            }

            // 8) Verify by re-GETting and comparing merged set
            let (stored_sibling_vecs, stored_vclock) =
            match self.connection.get_all_with_vclock::<Vec<String>>(Bucket::Variables, var).await {
                Ok((vecs, vc)) => (vecs, vc),
                Err(e) => {
                    if attempt < MAX_RETRIES - 1 {
                        continue;
                    }
                    return Err(e);
                }
            };

            let mut stored_set: HashSet<String> = HashSet::new();
            for sibling in stored_sibling_vecs {
                for id in sibling {
                    stored_set.insert(id);
                }
            }

            // Success: tx_id is no longer present
            if !stored_set.contains(&tx_id.to_string()) {
                return Ok(stored_vclock);
            }

            // else: a race occurred; retry
        }

        // Final check after retries
        let (final_sibling_vecs, final_vclock) =
        self.connection.get_all_with_vclock::<Vec<String>>(Bucket::Variables, var).await?;
        let mut final_set: HashSet<String> = HashSet::new();
        for sibling in final_sibling_vecs {
            for id in sibling {
                final_set.insert(id);
            }
        }

        if !final_set.contains(&tx_id.to_string()) {
            Ok(final_vclock)
        } else {
            Err(RiakError::UnexpectedStatus(409))
        }
    }

    pub async fn change_status(&mut self, new_state: TransactionStatus) -> Result<(), RiakError> {
        let bytes = postcard::to_stdvec(&new_state)
            .map_err(|e| RiakError::Serialize(e.to_string()))?;

        // Always GET the latest vclock before updating status
        // This ensures we handle any status siblings that may have been created
        let (_statuses, latest_vclock) = match self.connection
            .get_all_with_vclock::<TransactionStatus>(Bucket::Statuses, &self.id.to_string())
        .await
        {
            Ok((statuses, vc)) => (statuses, Some(vc)),
            // If status doesn't exist yet, that's fine
            Err(RiakError::UnexpectedStatus(404)) => (Vec::new(), None),
            Err(e) => return Err(e),
        };

        // Use the latest vclock from GET, not our cached one
        let _new_vclock = self
            .connection
            .put(Bucket::Statuses, &self.id.to_string(), bytes, latest_vclock)
        .await?;

        // FIXED: Do a verification GET to get the authoritative vclock
        // This ensures we always have a valid vclock, even on first write
        let (_verification_statuses, authoritative_vclock) = self.connection
            .get_all_with_vclock::<TransactionStatus>(Bucket::Statuses, &self.id.to_string())
        .await?;

        self.state = new_state;
        self.state_vclock = Some(authoritative_vclock);  // Use verified vclock
        Ok(())
    }

    // przyjmuje id zbioru proponowanych tx dla danej zmiennej
    // zwraca deterministycnie ta która "wygrała"
    // jej status zmini na 'approved'
    // a reszcie na 'rejected'
    pub async fn choose_tx(txs: &mut [Transaction<'a>]) -> Result<Uuid, RiakError> {
        assert!(!txs.is_empty(), "txs must not be empty");

        // deterministic target derived from "srds"
        let target = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"srds");

        let (winner_idx, _) = txs
            .iter()
            .enumerate()
            .min_by_key(|(_, tx)| uuid_distance(tx.id, target))
            .unwrap();

        let winner_id = txs[winner_idx].id;

        for (i, tx) in txs.iter_mut().enumerate() {
            if i == winner_idx {
                tx.approve().await?;
            } else {
                tx.reject().await?;
            }
        }

        Ok(winner_id)
    }
}

fn uuid_distance(a: Uuid, b: Uuid) -> u128 {
    a.as_u128().abs_diff(b.as_u128())
}

#[cfg(test)]
mod tests {
    // use super::*;

    // #[test]
    // fn it_works() {
    //     let ref client = Client::new("http://localhost:8098");
    //     let mut tx = Transaction::new(client);
    //     // let ob_url = r.object_url("test_bucket", "test_key");
    //     dbg!(&tx);
    //     // tx.write();
    //     // tx.read();
    //     // tx.commit();
    //     // tx.validate();
    //     // assert_eq!(ob_url, "http://test_url/types/default/buckets/test_bucket/keys/test_key");
    // }


    // #[tokio::test]
    // async fn test_get_returns_object_with_vclock() {
    //     let server = Server::run();
    //     let bucket = "mybucket";
    //     let key = "thekey";
    //     let body = b"hello riak".to_vec();
    //     let vclock = VClock(b"some-vclock-bytes".to_vec());
    //     let vclock_b64 = vclock.to_base64();
    //
    //     server.expect(
    //         Expectation::matching(all_of![
    //             request::method("GET"),
    //             request::path(format!("/types/default/buckets/{}/keys/{}", bucket, key))
    //         ])
    //         .respond_with(
    //             responders::status_code(200)
    //                 .append_header("X-Riak-Vclock", vclock_b64.clone())
    //                 .body(body.clone()),
    //         ),
    //     );
    //
    //     let client = Client::new(server.url("/").to_string());
    //     let obj = client.get(bucket, key).await.expect("get failed");
    //
    //     assert_eq!(obj.value, body);
    //     assert_eq!(obj.vclock, VClock(b"some-vclock-bytes".to_vec()));
    // }
}

