use std::{
    fs::{create_dir_all, File},
    io::{BufWriter, Write},
    path::PathBuf,
    time::Duration,
};

use etcd_proto::etcdserverpb::{
    CompactionRequest, DeleteRangeRequest, LeaseGrantRequest, LeaseLeasesRequest,
    LeaseRevokeRequest, LeaseTimeToLiveRequest, MemberAddRequest, MemberRemoveRequest,
    MemberUpdateRequest, PutRequest, RangeRequest, TxnRequest,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::info;

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
    MemberAdd(MemberAddRequest),
    MemberRemove(MemberRemoveRequest),
    MemberUpdate(MemberUpdateRequest),
    MemberList,
}

pub fn trace_task(
    file: PathBuf,
    mut shutdown: watch::Receiver<()>,
) -> (Option<JoinHandle<()>>, Option<mpsc::Sender<TraceValue>>) {
    let (send, mut recv) = mpsc::channel(10);
    info!(?file, "Creating parent directories for trace file");
    create_dir_all(file.parent().unwrap()).unwrap();
    let file_out = tokio::spawn(async move {
        let file = File::create(file).unwrap();
        info!(?file, "Created trace file");
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
