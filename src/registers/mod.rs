pub mod regular;

#[derive(Debug)]
pub enum RegisterIndication<T> {
    ReadReturn(T),
    WriteReturn,
}
