pub mod atomic;
pub mod regular;
pub mod logged_regular;

#[derive(Debug)]
pub enum RegisterIndication<T> {
    ReadReturn(Option<T>),
    WriteReturn,
}
