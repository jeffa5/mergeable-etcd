use automerge::{ObjId, ChangeHash};

pub trait ReadDoc: automerge::ReadDoc + autosurgeon::ReadDoc {
    fn hash_for_opid(&self, opid: &ObjId) -> Option<ChangeHash>;

    fn partial_cmp_heads(
        &self,
        heads1: &[ChangeHash],
        heads2: &[ChangeHash],
    ) -> Option<std::cmp::Ordering>;
}

impl ReadDoc for automerge::Automerge {
    fn hash_for_opid(&self, opid: &ObjId) -> Option<ChangeHash> {
        self.hash_for_opid(opid)
    }

    fn partial_cmp_heads(
        &self,
        heads1: &[ChangeHash],
        heads2: &[ChangeHash],
    ) -> Option<std::cmp::Ordering> {
        self.partial_cmp_heads(heads1, heads2)
    }
}
