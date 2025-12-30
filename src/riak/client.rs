use reqwest;
use crate::riak::object::{VClock, Object};
use reqwest::StatusCode;

#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    http: reqwest::Client
}

impl Client {

    pub fn new(base_url: impl Into<String>) -> Self {
        let base_url: String = base_url.into();
        let http = reqwest::Client::new();
        Self {base_url, http}
    }

    fn object_url(&self, bucket: &str, key: &str) -> String {
        let base = self.base_url.trim_end_matches('/');
        format!("{}/buckets/{}/keys/{}", base, bucket, key)
    }

    pub async fn get(&self, bucket: &str, key: &str) -> Result<Object, RiakError> {
        let url = self.object_url(bucket, key);
        let res = self.http.get(&url).send().await.map_err(RiakError::Http)?;

        let status = res.status();
        if !status.is_success() { // TODO: conflict siblings handler
            return Err(RiakError::UnexpectedStatus(status.as_u16()));
        }

        let headers = res.headers().clone();
        let bytes = res.bytes().await.map_err(RiakError::Http)?.to_vec();
        let vclock = VClock::from_headers(&headers)?;

        Ok(Object { value: bytes, vclock })
    }

    pub async fn put(
        &self,
        bucket: &str,
        key: &str,
        value: Vec<u8>,
        vclock: Option<VClock>,
    ) -> Result<VClock, RiakError> {
        let url = self.object_url(bucket, key);
        let mut req = self.http.put(&url);

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
}



#[derive(thiserror::Error, Debug)]
pub enum RiakError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("invalid vclock: {0}")]
    InvalidVClock(base64::DecodeError),
    #[error("invalid vclock header")]
    InvalidVClockHeader,
    #[error("unexpected status code: {0}")]
    UnexpectedStatus(u16),
}


#[cfg(test)]
mod tests {
    use super::*;
    use httptest::{Expectation, Server};
    use httptest::matchers::*;
    use httptest::matchers::request;
    use crate::riak::object::VClock;

    #[test]
    fn it_works() {
        let r = Client::new("test_url");
        let ob_url = r.object_url("test_bucket", "test_key");
        dbg!("{}", &ob_url);
        assert!(ob_url == "test_url/buckets/test_bucket/keys/test_key");
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
                request::path(format!("/buckets/{}/keys/{}", bucket, key))
            ])
            .respond_with(
                httptest::responders::status_code(200)
                    .append_header("X-Riak-Vclock", vclock_b64.clone())
                    .body(body.clone()),
            ),
        );

        let client = Client::new(server.url("").to_string());
        let obj = client.get(bucket, key).await.expect("get failed");

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
        let incoming_b64 = incoming_vclock.to_base64();

        let returned_vclock = VClock(b"returned-vc".to_vec());
        let returned_b64 = returned_vclock.to_base64();

        server.expect(
            Expectation::matching(all_of![
                request::method("PUT"),
                request::path(format!("/buckets/{}/keys/{}", bucket, key)),
                header("x-riak-vclock", incoming_b64.as_str())
            ])
            .respond_with(
                httptest::responders::status_code(204)
                    .append_header("X-Riak-Vclock", returned_b64.clone()),
            ),
        );

        let client = Client::new(server.url("").to_string());
        let new_vc = client
            .put(bucket, key, value, Some(incoming_vclock))
            .await
            .expect("put failed");

        assert_eq!(new_vc, returned_vclock);
    }
}
