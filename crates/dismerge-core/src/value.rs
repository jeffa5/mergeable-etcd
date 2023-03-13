use automerge::ScalarValue;

pub trait Value:
    Send
    + Sync
    + Clone
    + std::fmt::Debug
    + 'static
    + TryFrom<Vec<u8>>
    + Into<Vec<u8>>
    + Into<ScalarValue>
{
}

impl Value for Vec<u8> {}
