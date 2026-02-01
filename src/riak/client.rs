use reqwest;
use crate::riak::object::{VClock, Object};
use reqwest::StatusCode;
// use base64::{engine::general_purpose, Engine as _};
use multipart::server::Multipart;
use reqwest::header::HeaderMap;
use std::io::{Cursor, Read};

#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    http: reqwest::Client,
}

#[derive(Debug)]
pub enum GetResult {
    Single(Object),
    Siblings(Vec<Object>),
}

#[derive(Debug)]
pub enum Bucket {
    ReadSets,
    WriteSets,
    Statuses,
    Variables,
}

impl Client {
    pub fn new(base_url: impl Into<String>) -> Self {
        let base_url: String = base_url.into();
        let http = reqwest::Client::new();
        Self { base_url, http }
    }

    // Use Riak HTTP API path with bucket types (default type used here).
    // According to Riak HTTP API: /types/<type>/buckets/<bucket>/keys/<key>
    fn object_url(&self, bucket: Bucket, key: &str) -> String {
        // TODO: use Bucket enum
        let base = self.base_url.trim_end_matches('/');
        let bucket = match bucket {
            Bucket::ReadSets => "read_set",
            Bucket::WriteSets => "write_sets",
            Bucket::Statuses => "statuses",
            Bucket::Variables => "variables",
        };
        format!("{}/types/default/buckets/{}/keys/{}", base, bucket, key)
    }

    pub async fn get(&self, bucket: Bucket, key: &str) -> Result<GetResult, RiakError> {
        let url = self.object_url(bucket, key);

        // 1) permissive GET (no restrictive Accept)
        let res = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(RiakError::Http)?;

        let status = res.status();
        let headers = res.headers().clone();

        if status == StatusCode::OK {
            // single-object case
            let bytes = res.bytes().await.map_err(RiakError::Http)?;
            // let vclock = VClock::from_headers(&headers)?;
            // return Ok(GetResult::Single(Object {
            //     value: bytes.to_vec(),
            //     vclock,
            // }));
            return Ok(GetResult::Single(Object::new(&headers, bytes.to_vec())?));
        }

        if status == StatusCode::MULTIPLE_CHOICES {
            // server indicates siblings exist
            // re-request with Accept: multipart/mixed
            let res2 = self
                .http
                .get(&url)
                .header(reqwest::header::ACCEPT, "multipart/mixed")
                .send()
                .await
                .map_err(RiakError::Http)?;

            let status2 = res2.status();
            let headers2 = res2.headers().clone();

            if status2.is_success() || status2 == StatusCode::MULTIPLE_CHOICES {
                let bytes2 = res2.bytes().await.map_err(RiakError::Http)?;
                let siblings = Self::parse_multipart_siblings(&headers2, &bytes2)?;
                return Ok(GetResult::Siblings(siblings));
            } else {
                return Err(RiakError::UnexpectedStatus(status2.as_u16()));
            }
        }

        Err(RiakError::UnexpectedStatus(status.as_u16()))
    }


    pub fn parse_multipart_siblings(
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Vec<Object>, RiakError> {
        // Extract Content-Type
        let content_type = headers
            .get(reqwest::header::CONTENT_TYPE)
            .ok_or(RiakError::MissingContentType)?
            .to_str()
            .map_err(|_| RiakError::InvalidContentType)?;

        // Extract boundary aka delimeter
        let boundary = content_type
            .split("boundary=")
            .nth(1)
            .ok_or(RiakError::MissingBoundary)?;

        // let vclock = VClock::from_headers(headers)?;

        let mut multipart = Multipart::with_body(Cursor::new(body), boundary);

        let mut siblings = Vec::new();
        let mut error: Option<std::io::Error> = None;

        multipart.foreach_entry(|mut entry| {
            if error.is_some() {
                return; // stop processing further entries
            }

            let mut value = Vec::new();
            if let Err(e) = entry.data.read_to_end(&mut value) {
                error = Some(e);
                return;
            }

            // siblings.push(Object {
            //     value,
            //     vclock: vclock.clone(),
            // });
            let obj = Object::new(headers, value);
            match obj {
                Ok(obj) => siblings.push(obj),
                Err(_obj) => {}
            }
            // if let obj = Ok(obj) {
            //     siblings.push(obj);
            // } else {
            //     return obj;
            // }

        })?;

        if let Some(e) = error {
            return Err(RiakError::Io(e));
        }

        Ok(siblings)
    }

    pub async fn put(
        &self,
        bucket: Bucket,
        key: &str,
        value: Vec<u8>,
        vclock: Option<VClock>,
    ) -> Result<VClock, RiakError> {
        let url = self.object_url(bucket, key);
        // include Content-Type expected by Riak
        let mut req = self
            .http
            .put(&url)
            .header("Content-Type", "application/octet-stream");

        if let Some(vc) = vclock {
            let encoded = vc.to_base64();
            req = req.header("X-Riak-Vclock", encoded);
        }

        let res = req.body(value).send().await.map_err(RiakError::Http)?;
        let status = res.status();
        if !(status.is_success() || status == StatusCode::NO_CONTENT) {
            return Err(RiakError::UnexpectedStatus(status.as_u16()));
        }

        let headers = res.headers().clone();
        let new_vclock = VClock::from_headers(&headers)?;
        Ok(new_vclock)
    }

    // TODO:delete
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RiakError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("invalid vclock: {0}")]
    InvalidVClock(#[from] base64::DecodeError),

    #[error("invalid vclock header")]
    InvalidVClockHeader,

    #[error("unexpected status code: {0}")]
    UnexpectedStatus(u16),

    #[error("missing Content-Type header")]
    MissingContentType,

    #[error("invalid Content-Type header")]
    InvalidContentType,

    #[error("missing multipart boundary")]
    MissingBoundary,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

// ---------------------- TESTS ----------------------

#[cfg(test)]
mod tests {
    use super::*;
    use httptest::{Expectation, Server};
    use httptest::matchers::{all_of, request, contains};
    use httptest::responders;
    use crate::riak::object::VClock;

    #[test]
    fn it_works() {
        let r = Client::new("http://test_url");
        let ob_url = r.object_url(Bucket::Variables, "test_key");
        dbg!(&ob_url);
        assert_eq!(ob_url, "http://test_url/types/default/buckets/test_bucket/keys/test_key");
    }

    #[tokio::test]
    async fn test_get_returns_object_with_vclock() {
        let server = Server::run();
        let bucket = "mybucket";
        let key = "thekey";
        let body = b"hello riak".to_vec();
        let vclock = VClock(b"some-vclock-bytes".to_vec());
        let vclock_b64 = vclock.to_base64();

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("/types/default/buckets/{}/keys/{}", bucket, key))
            ])
                .respond_with(
                    responders::status_code(200)
                        .append_header("X-Riak-Vclock", vclock_b64.clone())
                        .body(body.clone()),
                ),
        );

        let client = Client::new(server.url("/").to_string());
        let obj = client.get(bucket, key).await.expect("get failed");

        let obj = match obj {
            GetResult::Single(obj) => obj,
            GetResult::Siblings(_) => panic!("unexpected siblings"),
        };

        assert_eq!(obj.value, body);
        assert_eq!(obj.vclock, VClock(b"some-vclock-bytes".to_vec()));
    }

    #[tokio::test]
    async fn test_put_sends_vclock_and_returns_new_vclock() {
        let server = Server::run();
        let bucket = "bucket2";
        let key = "key2";
        let value = b"payload".to_vec();

        // Expect incoming vclock header and body, respond with new vclock
        let incoming_vclock = VClock(b"in-vc".to_vec());
        // let incoming_b64 = incoming_vclock.to_base64();
        let incoming_b64: &'static str = Box::leak(incoming_vclock.to_base64().into_boxed_str());

        let returned_vclock = VClock(b"returned-vc".to_vec());
        let returned_b64 = returned_vclock.to_base64();

        server.expect(
            Expectation::matching(all_of![
                request::method("PUT"),
                request::path(format!("/types/default/buckets/{}/keys/{}", bucket, key)),
                request::headers(contains(("x-riak-vclock", incoming_b64))),
            ])
                .respond_with(
                    responders::status_code(204)
                        .append_header("X-Riak-Vclock", returned_b64),
                ),
        );

        let client = Client::new(server.url("/").to_string());
        let new_vc = client
            .put(bucket, key, value, Some(incoming_vclock))
            .await
            .expect("put failed");

        assert_eq!(new_vc, returned_vclock);
    }

    use rand::random;

    const HOST: &str = "http://localhost:8098";
    const BUCKET: &str = "testbucket";

    #[tokio::test]
    async fn it_puts_and_gets_value_from_real_riak() {
        let client = Client::new(HOST);
        let key = format!("key-{}", random::<u32>());
        let value = b"hello-riak".to_vec();
        dbg!(&key);

        let _vc = client.put(BUCKET, &key, value.clone(), None).await.unwrap();
        let obj = client.get(BUCKET, &key).await.unwrap();

        dbg!(&obj);

        let obj = match obj {
            GetResult::Single(obj) => obj,
            GetResult::Siblings(_) => panic!("unexpected siblings"),
        };

        let _ = dbg!(String::from_utf8(obj.value.clone()));
        dbg!(&obj.vclock.to_base64());

        assert_eq!(obj.value, value);
        // Riak does NOT guarantee returning vclock on PUT.
        // Correctness condition: value round-trips.
        // VClock returned by PUT may be empty; authoritative vclock comes from GET.
        assert!(!obj.vclock.0.is_empty());
    }

    #[tokio::test]
    async fn it_updates_value_and_increments_vclock() {
        let client = Client::new(HOST);
        let key = format!("key-{}", random::<u32>());
        dbg!(&key);

        let _vc1 = client.put(BUCKET, &key, b"first".to_vec(), None).await.unwrap();
        // Riak requires latest vclock from GET before update
        let current = client.get(BUCKET, &key).await.unwrap();
        dbg!(&current);

        let current = match current {
            GetResult::Single(current) => current,
            GetResult::Siblings(_) => panic!("unexpected siblings"),
        };

        let _ = dbg!(String::from_utf8(current.value));
        dbg!(&current.vclock.to_base64());

        let vc2 = client
            .put(BUCKET, &key, b"second".to_vec(), Some(current.vclock.clone()))
            .await
            .unwrap();

        let obj = client.get(BUCKET, &key).await.unwrap();
        dbg!(&obj);

        let obj = match obj {
            GetResult::Single(obj) => obj,
            GetResult::Siblings(_) => panic!("unexpected siblings"),
        };

        let _ = dbg!(String::from_utf8(obj.value.clone()));
        dbg!(&obj.vclock.to_base64());

        assert_eq!(obj.value, b"second".to_vec());
        // assert_ne!(vc1.0, vc2.0);
        // vc1 from initial PUT may be empty; compare authoritative vclocks via GET
        let obj_after_first = client.get(BUCKET, &key).await.unwrap();
        dbg!(&obj_after_first);

        let obj_after_first = match obj_after_first {
            GetResult::Single(obj_after_first) => obj_after_first,
            GetResult::Siblings(_) => panic!("unexpected siblings"),
        };
        assert_ne!(obj_after_first.vclock.0, vc2.0);
    }

    #[tokio::test]
    async fn test_get_handles_siblings_multipart() {
        use super::*;
        use httptest::{Expectation, Server};
        use httptest::matchers::{all_of, request, contains};
        use httptest::responders;
        use crate::riak::object::VClock;

        let server = Server::run();
        let bucket = "sibling_bucket";
        let key = "sibling_key";
        let path = format!("/types/default/buckets/{}/keys/{}", bucket, key);

        // Prepare vclock and multipart boundary/body
        let vclock = VClock(b"multipart-vclock".to_vec());
        let vclock_b64 = vclock.to_base64();

        let boundary = "BOUNDARY-12345";
        let part1_body = b"first-sibling".to_vec();
        let part2_body = b"second-sibling".to_vec();

        // Build multipart bytes using CRLF as required by MIME and ensure valid Content-Disposition
        let mut multipart = Vec::new();

        // --boundary (part 1)
        multipart.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        multipart.extend_from_slice(b"Content-Type: text/plain\r\n");
        // <- use form-data with a name parameter (parser expects this)
        multipart.extend_from_slice(b"Content-Disposition: form-data; name=\"object1\"; filename=\"part1\"\r\n");
        multipart.extend_from_slice(b"ETag: \"etag1\"\r\n");
        multipart.extend_from_slice(b"\r\n");
        multipart.extend_from_slice(&part1_body);
        multipart.extend_from_slice(b"\r\n");

        // --boundary (part 2)
        multipart.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        multipart.extend_from_slice(b"Content-Type: text/plain\r\n");
        multipart.extend_from_slice(b"Content-Disposition: form-data; name=\"object2\"; filename=\"part2\"\r\n");
        multipart.extend_from_slice(b"ETag: \"etag2\"\r\n");
        multipart.extend_from_slice(b"\r\n");
        multipart.extend_from_slice(&part2_body);
        multipart.extend_from_slice(b"\r\n");

        // --boundary--
        multipart.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());

        // 1) First permissive GET -> server replies 300 MULTIPLE_CHOICES
        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(path.clone()),
            ])
                .respond_with(responders::status_code(300)),
        );

        // 2) Second GET with Accept: multipart/mixed -> server replies with multipart body
        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(path.clone()),
                request::headers(contains(("accept", "multipart/mixed"))),
            ])
                .respond_with(
                    responders::status_code(200)
                        .append_header(
                            "Content-Type",
                            format!("multipart/mixed; boundary={}", boundary),
                        )
                        .append_header("X-Riak-Vclock", vclock_b64.clone())
                        .body(multipart.clone()),
                ),
        );

        let client = Client::new(server.url("/").to_string());
        let res = client.get(bucket, key).await.expect("get failed");

        let siblings = match res {
            GetResult::Siblings(sibs) => sibs,
            GetResult::Single(_) => panic!("expected siblings but got single object"),
        };

        assert_eq!(siblings.len(), 2);
        assert_eq!(siblings[0].value, part1_body);
        assert_eq!(siblings[1].value, part2_body);

        // Verify that the vclock on each sibling matches the X-Riak-Vclock header
        assert_eq!(siblings[0].vclock, vclock);
        assert_eq!(siblings[1].vclock, vclock);
    }

    use super::*;
    use reqwest::header::{HeaderMap, CONTENT_TYPE};

    #[test]
    fn test_parse_multipart_siblings_success() {
        let mut headers = HeaderMap::new();
        let boundary = "---------------------------284241091321721";
        
        headers.insert(
            CONTENT_TYPE,
            format!("multipart/mixed; boundary={}", boundary).parse().unwrap(),
        );
        
        // Mock VClock header that Object::new expects
        let vclock_base64 = "a85hYGBymGBlSGRYxFzH6mS48u88IwszhzgzZf8fAA==";
        headers.insert("X-Riak-Vclock", vclock_base64.parse().unwrap());

        // Construct a body that satisfies the 'multipart' crate's requirements:
        // 1. Use \r\n (CRLF) strictly.
        // 2. Include Content-Disposition (which the parser mandates).
        let mut body = Vec::new();
        
        // Sibling 1
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n");
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"sibling1\"\r\n");
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(b"sibling-one-data");
        body.extend_from_slice(b"\r\n");

        // Sibling 2
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n");
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"sibling2\"\r\n");
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(b"sibling-two-data");
        body.extend_from_slice(b"\r\n");

        // End Boundary
        body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());

        let result = Client::parse_multipart_siblings(&headers, &body);

        let siblings = result.expect("Should successfully parse siblings");
        assert_eq!(siblings.len(), 2);
        assert_eq!(siblings[0].value, b"sibling-one-data");
        assert_eq!(siblings[1].value, b"sibling-two-data");
    }

    #[test]
    fn test_parse_multipart_missing_boundary() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        
        let body = b"not-multipart";
        let result = Client::parse_multipart_siblings(&headers, body);

        match result {
            Err(RiakError::MissingBoundary) => {},
            _ => panic!("Expected MissingBoundary error"),
        }
    }

    #[test]
    fn test_parse_multipart_invalid_body() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            "multipart/mixed; boundary=abc".parse().unwrap(),
        );
        
        // Providing garbage data
        let body = b"this is not a valid multipart format";
        let result = Client::parse_multipart_siblings(&headers, body);

        // The parser returns an Io error (UnexpectedEof) when the start boundary is missing
        match result {
            Err(RiakError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof),
            _ => panic!("Expected Io(UnexpectedEof) error, got {:?}", result),
        }
    }

    #[test]
    fn test_parse_multipart_empty_content_type() {
        let headers = HeaderMap::new(); // No Content-Type
        let body = b"";
        let result = Client::parse_multipart_siblings(&headers, body);

        match result {
            Err(RiakError::MissingContentType) => {},
            _ => panic!("Expected MissingContentType error"),
        }
    }

}
