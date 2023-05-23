use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputData<D: Default>(Output, D);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Output {
    pub start_ns: i64,
    pub end_ns: i64,
    pub error: Option<String>,
    pub client: u32,
    pub iteration: u32,
}

impl<D: Default> OutputData<D> {
    pub fn start(client: u32, iteration: u32) -> Self {
        let now = chrono::Utc::now();
        Self(
            Output {
                start_ns: now.timestamp_nanos(),
                end_ns: now.timestamp_nanos(),
                error: None,
                client,
                iteration,
            },
            D::default(),
        )
    }

    pub fn stop(&mut self) {
        self.0.end_ns = chrono::Utc::now().timestamp_nanos();
    }

    pub fn error(&mut self, error: String) {
        self.0.error = Some(error);
        self.0.end_ns = chrono::Utc::now().timestamp_nanos();
    }

    pub fn is_error(&self) -> bool {
        self.0.error.is_some()
    }

    pub fn data_mut(&mut self) -> &mut D {
        &mut self.1
    }
}

impl<D: Default> Drop for OutputData<D> {
    fn drop(&mut self) {
        if self.0.end_ns == self.0.start_ns {
            self.stop()
        }
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct EtcdOutput {
    pub endpoint: String,
    pub key: String,
    pub member_id: u64,
    pub raft_term: u64,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct DismergeOutput {
    pub endpoint: String,
    pub key: String,
    pub member_id: u64,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct SleepOutput {}
