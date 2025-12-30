use crate::riak::client::RiakError;
use base64::{engine::general_purpose, Engine as _};
use reqwest::header::HeaderMap;

pub struct Object {
    pub value: Vec<u8>,
    pub vclock: VClock,
}


#[derive(Clone)]
pub struct VClock(pub Vec<u8>);

impl VClock {
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
