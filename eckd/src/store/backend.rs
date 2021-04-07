#[derive(Debug)]
pub struct Backend {
    db: sled::Db,
    backend: automerge_persistent::PersistentBackend<automerge_persistent_sled::SledPersister>,
}

impl Backend {
    pub fn new(config: &sled::Config) -> Self {
        let db = config.open().unwrap();
        let changes_tree = db.open_tree("changes").unwrap();
        let document_tree = db.open_tree("document").unwrap();
        let sled_perst = automerge_persistent_sled::SledPersister::new(
            changes_tree,
            document_tree,
            String::new(),
        );
        let backend = automerge_persistent::PersistentBackend::load(sled_perst).unwrap();

        Self { db, backend }
    }
}
