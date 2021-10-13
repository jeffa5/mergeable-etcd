use std::{
    fs::{create_dir_all, File},
    io::{BufWriter, Write},
    path::PathBuf,
    time::Duration,
};

use etcd_proto::etcdserverpb::{
    CompactionRequest, DeleteRangeRequest, LeaseGrantRequest, LeaseLeasesRequest,
    LeaseRevokeRequest, LeaseTimeToLiveRequest, PutRequest, RangeRequest, TxnRequest,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

#[derive(serde::Serialize, serde::Deserialize)]
pub enum TraceValue {
    RangeRequest(RangeRequest),
    PutRequest(PutRequest),
    DeleteRangeRequest(DeleteRangeRequest),
    TxnRequest(TxnRequest),
    CompactRequest(CompactionRequest),
    LeaseGrantRequest(LeaseGrantRequest),
    LeaseRevokeRequest(LeaseRevokeRequest),
    LeaseTimeToLiveRequest(LeaseTimeToLiveRequest),
    LeaseLeasesRequest(LeaseLeasesRequest),
}

pub fn trace_task(
    file: PathBuf,
    mut shutdown: watch::Receiver<()>,
) -> (Option<JoinHandle<()>>, Option<mpsc::Sender<TraceValue>>) {
    let (send, mut recv) = mpsc::channel(10);
    let file_out = tokio::spawn(async move {
        create_dir_all(file.parent().unwrap()).unwrap();

        let file = File::create(file).unwrap();
        let mut bw = BufWriter::new(file);
        let mut i = 0;
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    bw.flush().unwrap()
                },
                Some(tv) = recv.recv() => {
                    let dt = chrono::Utc::now();
                    let b = serde_json::to_string(&tv).unwrap();

                    writeln!(bw, "{} {}", dt.to_rfc3339(), b).unwrap();

                    i += 1;
                    if i % 100 == 0 {
                        bw.flush().unwrap()
                    }
                }
            }
        }
        bw.flush().unwrap()
    });
    (Some(file_out), Some(send))
}
