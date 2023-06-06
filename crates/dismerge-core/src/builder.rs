use std::collections::HashMap;
use std::marker::PhantomData;

use automerge_persistent::{MemoryPersister, PersistentAutomerge, Persister};
use rand::SeedableRng;
use rand::{rngs::StdRng, Rng};
use tokio::sync::watch;

use crate::value::Value;
use crate::{Document, Syncer, Watcher};

pub struct DocumentBuilder<P, S, W, V> {
    persister: P,
    syncer: S,
    watcher: W,
    cluster_id: Option<u64>,
    member_id: u64,
    name: String,
    peer_urls: Vec<String>,
    client_urls: Vec<String>,
    seed: u64,
    auto_flush: bool,
    auto_sync: bool,
    max_outstanding: u64,
    _value_type: PhantomData<V>,
}

impl<V> Default for DocumentBuilder<MemoryPersister, (), (), V> {
    fn default() -> Self {
        Self {
            persister: MemoryPersister::default(),
            syncer: (),
            watcher: (),
            cluster_id: None,
            member_id: 1,
            name: "default".to_owned(),
            peer_urls: vec![],
            client_urls: vec![],
            seed: rand::thread_rng().gen(),
            auto_flush: true,
            auto_sync: true,
            max_outstanding: 1_000,
            _value_type: PhantomData::default(),
        }
    }
}

impl<P, S, W, V> DocumentBuilder<P, S, W, V> {
    #[must_use]
    pub fn with_persister<P2>(self, persister: P2) -> DocumentBuilder<P2, S, W, V> {
        DocumentBuilder {
            persister,
            syncer: self.syncer,
            watcher: self.watcher,
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            name: self.name,
            peer_urls: self.peer_urls,
            client_urls: self.client_urls,
            seed: self.seed,
            auto_flush: self.auto_flush,
            auto_sync: self.auto_sync,
            max_outstanding: self.max_outstanding,
            _value_type: PhantomData::default(),
        }
    }

    #[must_use]
    pub fn with_syncer<S2>(self, syncer: S2) -> DocumentBuilder<P, S2, W, V> {
        DocumentBuilder {
            persister: self.persister,
            syncer,
            watcher: self.watcher,
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            name: self.name,
            peer_urls: self.peer_urls,
            client_urls: self.client_urls,
            seed: self.seed,
            auto_flush: self.auto_flush,
            auto_sync: self.auto_sync,
            max_outstanding: self.max_outstanding,
            _value_type: PhantomData::default(),
        }
    }

    #[must_use]
    pub fn with_watcher<W2>(self, watcher: W2) -> DocumentBuilder<P, S, W2, V> {
        DocumentBuilder {
            persister: self.persister,
            syncer: self.syncer,
            watcher,
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            name: self.name,
            peer_urls: self.peer_urls,
            client_urls: self.client_urls,
            seed: self.seed,
            auto_flush: self.auto_flush,
            auto_sync: self.auto_sync,
            max_outstanding: self.max_outstanding,
            _value_type: PhantomData::default(),
        }
    }

    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: u64) -> Self {
        self.cluster_id = Some(cluster_id);
        self
    }

    pub fn set_cluster_id(&mut self, cluster_id: u64) -> &mut Self {
        self.cluster_id = Some(cluster_id);
        self
    }

    #[must_use]
    pub fn with_member_id(mut self, member_id: u64) -> Self {
        self.member_id = member_id;
        self
    }

    pub fn set_member_id(&mut self, member_id: u64) -> &mut Self {
        self.member_id = member_id;
        self
    }

    #[must_use]
    pub fn with_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    pub fn set_name(&mut self, name: String) -> &mut Self {
        self.name = name;
        self
    }

    #[must_use]
    pub fn with_peer_urls(mut self, peer_urls: Vec<String>) -> Self {
        self.peer_urls = peer_urls;
        self
    }

    pub fn set_peer_urls(&mut self, peer_urls: Vec<String>) -> &mut Self {
        self.peer_urls = peer_urls;
        self
    }

    #[must_use]
    pub fn with_client_urls(mut self, client_urls: Vec<String>) -> Self {
        self.client_urls = client_urls;
        self
    }

    pub fn set_client_urls(&mut self, client_urls: Vec<String>) -> &mut Self {
        self.client_urls = client_urls;
        self
    }

    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn set_seed(&mut self, seed: u64) -> &mut Self {
        self.seed = seed;
        self
    }

    #[must_use]
    pub fn with_auto_flush(mut self, auto_flush: bool) -> Self {
        self.auto_flush = auto_flush;
        self
    }

    pub fn set_auto_flush(&mut self, auto_flush: bool) -> &mut Self {
        self.auto_flush = auto_flush;
        self
    }

    #[must_use]
    pub fn with_auto_sync(mut self, auto_sync: bool) -> Self {
        self.auto_sync = auto_sync;
        self
    }

    pub fn set_auto_sync(&mut self, auto_sync: bool) -> &mut Self {
        self.auto_sync = auto_sync;
        self
    }

    #[must_use]
    pub fn with_max_outstanding(mut self, max_outstanding: u64) -> Self {
        self.max_outstanding = max_outstanding;
        self
    }

    pub fn set_max_outstanding(&mut self, max_outstanding: u64) -> &mut Self {
        self.max_outstanding = max_outstanding;
        self
    }
}

impl<S, W, V> DocumentBuilder<MemoryPersister, S, W, V> {
    pub fn with_in_memory(mut self) -> Self {
        self.persister = MemoryPersister::default();
        self
    }

    pub fn set_in_memory(&mut self) -> &mut Self {
        self.persister = MemoryPersister::default();
        self
    }
}

impl<P, S, W, V> DocumentBuilder<P, S, W, V>
where
    P: Persister + 'static,
    S: Syncer,
    W: Watcher<V>,
    V: Value,
{
    #[must_use]
    pub fn build(self) -> Document<P, S, W, V> {
        let am = PersistentAutomerge::load(self.persister).unwrap();
        let (flush_notifier, flush_notifier_receiver) = watch::channel(());
        let mut s = Document {
            am,
            member_id: self.member_id,
            name: self.name,
            peer_urls: self.peer_urls,
            client_urls: self.client_urls,
            syncer: self.syncer,
            watcher: self.watcher,
            kvs_objid: automerge::ObjId::Root,
            members_objid: automerge::ObjId::Root,
            leases_objid: automerge::ObjId::Root,
            cluster_objid: automerge::ObjId::Root,
            rng: StdRng::seed_from_u64(self.seed),
            flush_notifier,
            flush_notifier_receiver,
            auto_flush: self.auto_flush,
            auto_sync: self.auto_sync,
            outstanding: 0,
            max_outstanding: self.max_outstanding,
            _value_type: PhantomData::default(),
            peer_heads: HashMap::default(),
        };
        s.init(self.cluster_id);
        s
    }
}
