use std::collections::HashMap;
use uuid::Uuid;
use crate::riak::client::Client;
use crate::riak::client::Bucket;
use crate::riak::client::GetResult;

#[derive(Debug)]
enum TransactionStatus {
    Created,
    Prepred,
    Commited,
    Proposed,
    Approved,
    Rejected
}


#[derive(Debug)]
struct Transaction<'a> {
    id: Uuid,
    read_set: HashMap<String, Uuid>, // dependecy set
    write_set: HashMap<String, Vec<u8>>,
    state: TransactionStatus,
    connection: &'a Client,
    // TODO: timestamp proposed ?
    // TODO: timestamp Approved ?
}

impl<'a> Transaction<'a> {
    fn new(connection: &'a Client) -> Self {
        let id = Uuid::new_v4();
        let read_set: HashMap<String,Uuid> = HashMap::new();
        let write_set: HashMap<String,Vec<u8>> = HashMap::new();
        let state: TransactionStatus = TransactionStatus::Created;
        // let client = conn;

        Transaction {
            id,
            read_set,
            write_set,
            state,
            connection
        }
    }

    fn from_uuid(id: Uuid, connection: &'a Client) -> Self {
        // TODO: get real values
        let read_set: HashMap<String,Uuid> = HashMap::new(); 
        let write_set: HashMap<String,Vec<u8>> = HashMap::new();
        let state: TransactionStatus = TransactionStatus::Created;

        Self {
            id,
            read_set,
            write_set,
            state,
            connection
        }
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

    fn find_dependency(&self, key: &str) -> Option<Uuid> {
        let GetResult(predecessors) = self.connection.get(Bucket::Statuses, key).await else {
            return None;
        };
        
        // jeśli nie ma to zracamy None
        // walidacja swojego reada, potencjalnie wiecej niz jedna tx dla zmiennej key:
        // dla każdej tx musimy pobrać jej rodzinów
        // dla każdej zmiennej która występuje w read_set tych rodzicach pobieramy tx
        // jeśli jeden to wygrywa i jeśli już nie jest approved to change_status(Approved)
        // jeśli więcej to:
        // 0. pomijamy jak one same sa REJECTED
        // 1. reject wszystkie które nie zawirają naszej zmiennej key
        // 2. reject wszystkie których którykolwiek rodzic  
        // 3. szukamy Approved jeśli kilka to najwcześniejszego
        // 4. jeśli nie ma approved to wybieramy tego przy pomocy choose_tx
    }

    fn read(&mut self, key: &str) -> Option<Vec<u8>> {
        // TODO: riak client get, without creating whole Transaciont object
        if let Some(p_id) = self.find_dependency(key) {
            let p_tx = Transaction::from_uuid(p_id, self.connection);
            self.read_set.insert(key.to_string(), p_id);
            p_tx.write_set.get(key).cloned()
        } else {
            None
        }
    }

    fn commit(&mut self) {
        // TODO: raik put for write_set
        self.change_status(TransactionStatus::Commited);


    }
    

    fn change_status(&mut self, new_status: TransactionStatus) {
        // TODO: raik update
        // match new_status {
        //     
        //
        // };
        self.state = new_status;
    }
}


// przyjmuje id zbioru proponowanych tx dla danej zmiennej
// zwraca ta która "wygrała"
// jej status zmini na 'approved'
// a reszcie na 'rejected'
fn choose_tx(ids: Vec<Uuid>) -> Uuid {
    let target = "srds0000-0000-0000-0000-000000000000";

    ids.into_iter()
        .min_by_key(|id| {
            let s = id.to_string();

            s.chars()
                .zip(target.chars())
                .map(|(a, b)| (a as i32 - b as i32).abs())
                .sum::<i32>()
        })
        .expect("ids must not be empty")
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

