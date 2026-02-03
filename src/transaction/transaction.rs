use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use crate::riak::client::Client;
use crate::riak::client::Bucket;
use crate::riak::client::RiakError;
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
    connection: &'a Client,
}

impl<'a> Transaction<'a> {
    fn new(connection: &'a Client) -> Self {
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
            connection
        }
    }

    async fn from_uuid(id: Uuid, connection: &'a Client) -> Result<Self, RiakError> {
        // TODO: 
        // let read_set: HashMap<String,Uuid> = connection.get(Bucket::ReadSets, &id.to_string()).await; 
        // let write_set: HashMap<String,Vec<u8>> = connection.get(Bucket::WriteSets, &id.to_string()).await;
        // let state: TransactionStatus = connection.get(Bucket::Statuses, &id.to_string()).await;
        let read_set: HashMap<String, Uuid> = connection.get_deserialized(Bucket::ReadSets, &id.to_string()).await?;
        let write_set: HashMap<String, Vec<u8>> = connection.get_deserialized(Bucket::WriteSets, &id.to_string()).await?;

        let possible_states: Vec<TransactionStatus> = connection.get_all_deserialized(Bucket::Statuses, &id.to_string()).await?;
        let state: TransactionStatus = {
            // wybieramy pierwszy Rejected lub Approved
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
            connection
        })
    }

    fn write(&mut self, key: &str, value: Vec<u8> ) {
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
        // dostajemy liste tx_id których nie widzielimy
        let possible_dependencies_ids: Vec<String> = self.connection.get_all_deserialized(Bucket::Variables, key).await?;

        // jeśli nie ma to zracamy None
        if possible_dependencies_ids.is_empty() {
            return Ok(None);
        }

        // MARK: walidacja swojego reada, potencjalnie wiecej niz jedna tx dla zmiennej key:
        // dla każdej tx musimy pobrać jej rodzinów i ich zmienne
        let mut parents: HashSet<Uuid> = HashSet::new();
        let mut variables: HashSet<String> = HashSet::new();

        for pd_id in possible_dependencies_ids {
            let read_set_result: Vec<HashMap<String, Uuid>> = self.connection.get_deserialized(Bucket::ReadSets, &pd_id).await?;
            for rs in read_set_result {
                parents.extend(rs.values().cloned());
                variables.extend(rs.keys().cloned());
            }
        } 

        // dla każdej zmiennej która występuje w read_set tych rodzicach pobieramy tx
        let tx_ids: HashSet<String> = {
            let mut set = HashSet::new();
            for var in &variables {
                set.extend(
                    self.connection
                        .get_all_deserialized::<String>(Bucket::Variables, var)
                        .await?
                );
            }
            set
        };

        // let txs: Vec<Transaction> = {
        //     let mut v = Vec::new();
        //     for id in tx_ids {
        //         v.push({
        //             let uuid = Uuid::try_parse(&id).expect("Invalid UUId");
        //             Transaction::from_uuid(uuid, self.connection).await?
        //         });
        //     }
        //     v
        // };
        let tx_futures = tx_ids.into_iter().map(|id| {
            let uuid = Uuid::try_parse(&id).map_err(|_| RiakError::InvalidId);
            async move {
                Transaction::from_uuid(uuid?, self.connection).await
            }
        });
        let txs: Vec<Transaction> = futures::future::try_join_all(tx_futures).await?;

        // jeśli jeden to wygrywa i jeśli już nie jest approved to change_status(Approved)
        if txs.len() == 1 {
            let mut winner = txs.into_iter().next().unwrap();
            winner.approve();
            return Ok(Some(winner.id));
        }

        // bierzemy wszystkich możliwych parentów pomoże nam to znaleść frontline 
        // - nie jest to chyba jednak potrzebne 
        // bo wystrzczy że bedziemy patrzeć czy parent jest w latest
        // let all_parents: HashSet<Uuid> = {
        //     let mut v = HashSet::new();
        //     for tx in &txs {
        //         v.extend(tx.read_set.values().copied());
        //     }
        //     v
        // };

        // wyfiltrowane parents pomogą znaleść które transakcjie z frontlinu są aktualne
        let latest_parents: HashMap<String, Uuid> = {
            // dla każdej zmiennej musimy znaleść Approved z najpóźniejszym timestampem
            // variable -> (parent_tx_id, state_timestamp)
            let mut latest: HashMap<String, (Uuid, DateTime<Utc>)> = HashMap::new();

            for tx in &txs {
                // only final states participate
                if !matches!(tx.state, TransactionStatus::Approved { .. }) {
                    continue;
                }

                let at = tx.state.at();

                for (var, parent_id) in &tx.read_set {
                    match latest.get(var) {
                        Some((_, existing_at)) if *existing_at >= at => {
                            // existing final state is newer → keep it
                        }
                        _ => {
                            // this final state is newer → replace
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


        // let mut parent_statuses: HashMap<Uuid, TransactionStatus> = HashMap::new();
        //
        // for parent in &all_parents {
        //     let status = self.connection
        //         .get_deserialized(Bucket::Statuses, &parent.to_string())
        //     .await?;
        //     parent_statuses.insert(*parent, status);
        // }

        // jeśli więcej to:
        let mut potential_dependencies: Vec<Transaction> = Vec::new();
        //
        // Wykluczanie:
        'txs: for mut tx in txs {
            // 0. pomijamy jak one same sa REJECTED
            if matches!(tx.state, TransactionStatus::Rejected { .. }) {
                continue;
            }
            // 1. reject wszystkie których którykolwiek rodzic jest Rejected
            //    to w sumie jest nie potrzebne bo 3.
            //    i dobrze bo to by było bardzo bardzo kosztowne
            // for parent_uuid in tx.read_set.values() {
            //     let parent_status = self.connection.get_deserialized(Bucket::Statuses, &parent_uuid.to_string()).await?;
            //     if matches!(parent_status, TransactionStatus::Rejected { .. }) {
            //         tx.reject();
            //         continue 'txs; // continue outer loop
            //     }
            // }
            // 2. reject wszystkie które nie zawirają naszej zmiennej key
            if !tx.read_set.contains_key(key) {
                tx.reject();
                continue 'txs;
            }

            // 3. reject wszystkie które nie mają parenta w latest_parents dla danej zmiennej
            match latest_parents.get(key) {
                Some(latest_parent) => {
                    match tx.read_set.get(key) {
                        Some(tx_parent) if tx_parent == latest_parent => {
                            // OK — tx is on the frontline for this key
                        }
                        _ => {
                            tx.reject();
                            continue 'txs;
                        }
                    }
                }
                None => {
                    // no approved parent exists for this variable
                    // TODO: co jeśli nie ma parenta i zmienna jest nowa
                    // póki co init z T0 i zmiennymi których moża używać
                    tx.reject();
                    continue 'txs;
                }
            }
            
            // nie spełnił żandego warunku wyklucznia
            // - to znaczy że jest na frontline 
            // - jego każdy parent jest approved
            // - jego każdy parent jest 'najświerzszy'
            // - zawiera nasz zmienną key
            //
            // więc wpisujemy do grona do wyboru
            potential_dependencies.push(tx);
        };

        // jeśli nie ma to wygrywa ostatni parent dla zmiennej
        if potential_dependencies.is_empty() {
            let Some(&winner) = latest_parents.get(key) else {
                return Ok(None);
            };
            return Ok(Some(winner));
        }

        // jeśli jeden to wygrywa i jeśli już nie jest approved to change_status(Approved)
        if potential_dependencies.len() == 1 {
            let mut winner = potential_dependencies.into_iter().next().unwrap();
            winner.approve();
            return Ok(Some(winner.id));
        }
        // Znajdowanie:
        // 1. szukamy Approved jeśli kilka to najwcześniejszego
        if let Some(winner) = potential_dependencies
            .iter()
            .filter(|tx| matches!(tx.state, TransactionStatus::Approved { .. }))
            .min_by_key(|tx| tx.state.at())
        {
            return Ok(Some(winner.id));
        }

        // 2. jeśli nie ma approved to wybieramy tego przy pomocy choose_tx
        let winner = Transaction::choose_tx(&mut potential_dependencies);
        Ok(Some(winner))
    }

    // let GetResult::Siblings(possible_dependencies) = possible_dependencies else {
    //     return Some(Uuid::parse_str(possible_dependencies.into()).expect("Invalid UUID"));
    // };

    async fn read(&mut self, key: &str) -> Result<Option<Vec<u8>>, RiakError> {
        // TODO: riak client get, without creating whole Transaciont object
        if let Some(p_id) = self.find_dependency(key).await? {
            let p_tx = Transaction::from_uuid(p_id, self.connection).await?;
            self.read_set.insert(key.to_string(), p_id);
            Ok(p_tx.write_set.get(key).cloned())
        } else {
            Ok(None)
        }
    }

    // fn change_status(&mut self, new_status: TransactionStatus) {
    //     // TODO: raik update
    //     // match new_status {
    //     // };
    //     self.state = new_status;
    // }

    fn commit(&mut self) {
        // change to Committed
        // TODO: raik put for write_set

    }

    fn reject(&mut self) {
        // change to Rejected

    }

    fn approve(&mut self) {
        // change to Aproved 

    }

    // przyjmuje id zbioru proponowanych tx dla danej zmiennej
    // zwraca deterministycnie ta która "wygrała"
    // jej status zmini na 'approved'
    // a reszcie na 'rejected'
    fn choose_tx(txs: &mut [Transaction]) -> Uuid {
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
                tx.approve();
            } else {
                tx.reject();
            }
        }

        winner_id
    }
}

fn uuid_distance(a: Uuid, b: Uuid) -> u128 {
    let a = a.as_u128();
    let b = b.as_u128();
    if a > b { a - b } else { b - a }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let ref client = Client::new("http://localhost:8098");
        let mut tx = Transaction::new(client);
        // let ob_url = r.object_url("test_bucket", "test_key");
        dbg!(&tx);
        // tx.write();
        // tx.read();
        // tx.commit();
        // tx.validate();
        // assert_eq!(ob_url, "http://test_url/types/default/buckets/test_bucket/keys/test_key");
    }


    #[test]
    fn filip_it_works() {

        fn sth(a: u8) -> bool {
            if a > 3 {
                // let b = if a == 2 { true } else { false };
                // b
                match a {
                    2 => true,
                    _ => false
                }
            } else {
                false
            }
        }

        sth(3);
    }

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

