pub mod atomic;
pub mod regular;

#[derive(Debug)]
pub enum RegisterIndication<T> {
    ReadReturn(Option<T>),
    WriteReturn,
}
