use crate::riak::client::RiakError;
use base64::{engine::general_purpose, Engine as _};
use reqwest::header::HeaderMap;

#[derive(Debug)]
pub struct Object {
    pub value: Vec<u8>,
    pub vclock: VClock,                // shared across siblings
    pub vtag: Option<String>,          // per-sibling ETag / vtag
    pub content_type: Option<String>,  // per-sibling Content-Type
    pub last_modified: Option<String>, // per-sibling Last-Modified
}

impl Object {
    pub fn new(headers: &HeaderMap, body: Vec<u8>) -> Result<Object, RiakError> {
        let vclock = VClock::from_headers(headers)?;

        let vtag = headers
            .get("ETag")
            .or_else(|| headers.get("Etag"))
            .and_then(|s| s.to_str().ok())
            .map(|s| s.to_string());

        let content_type = headers
            .get("Content-Type")
            .and_then(|s| s.to_str().ok())
            .map(|s| s.to_string());

        let last_modified = headers
            .get("Last-Modified")
            .and_then(|s| s.to_str().ok())
            .map(|s| s.to_string());

        Ok(Object {
            value: body,
            vclock,
            vtag,
            content_type,
            last_modified,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VClock(pub Vec<u8>);

impl VClock {
    pub fn new() -> Self {
        VClock(vec![0; 8]) // 8 zeros, or whatever length you want
    }

    pub fn to_base64(&self) -> String {
        general_purpose::STANDARD.encode(&self.0)
    }

    pub fn from_base64(s: &str) -> Result<Self, RiakError> {
        let decoded = general_purpose::STANDARD
            .decode(s)
            .map_err(RiakError::InvalidVClock)?;
        Ok(VClock(decoded))
    }


    pub fn from_headers(headers: &HeaderMap) -> Result<Self, RiakError> {
        if let Some(v) = headers.get("X-Riak-Vclock") {
            let v_str = v.to_str().map_err(|_| RiakError::InvalidVClockHeader)?;
            // Riak often uses binary vclocks; some servers send base64 — try base64 first,
            // otherwise treat header bytes directly.
            if let Ok(vc) = general_purpose::STANDARD.decode(v_str) {
                return Ok(VClock(vc));
            }
            // fallback: raw header bytes
            return Ok(VClock(v.as_bytes().to_vec()));
        }
        Ok(VClock(Vec::new()))
    }
}

impl Default for VClock {
    fn default() -> Self {
        Self::new()
    }
}



