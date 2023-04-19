use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Output<D> {
    pub start_ns: i64,
    pub end_ns: i64,
    pub error: Option<String>,
    pub client: u32,
    pub iteration: u32,
    pub data: Option<D>,
}

impl<D> Output<D> {
    pub fn start(client: u32, iteration: u32) -> Self {
        let now = chrono::Utc::now();
        Self {
            start_ns: now.timestamp_nanos(),
            end_ns: now.timestamp_nanos(),
            error: None,
            client,
            iteration,
            data: None,
        }
    }

    pub fn stop(&mut self) {
        self.end_ns = chrono::Utc::now().timestamp_nanos();
    }

    pub fn error(&mut self, error: String) {
        self.error = Some(error);
        self.end_ns = chrono::Utc::now().timestamp_nanos();
    }

    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

impl<D> Drop for Output<D> {
    fn drop(&mut self) {
        if self.end_ns == self.start_ns {
            self.stop()
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EtcdOutput {
    pub key: String,
    pub member_id: u64,
    pub raft_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DismergeOutput {
    pub key: String,
    pub member_id: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SleepOutput {}
