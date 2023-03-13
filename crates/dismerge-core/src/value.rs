pub trait Value: Send + Clone + std::fmt::Debug + 'static + TryFrom<Vec<u8>> {}
