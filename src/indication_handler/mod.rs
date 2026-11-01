use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

// type AsyncCallback = Box<dyn AsyncFn(&dyn Any)>;

// pub struct IndicationHandler<U> {
//     async_handlers: HashMap<TypeId, Vec<AsyncCallback>>,
// }

// impl<U> IndicationHandler<U> {
//     pub fn add_async_handler<T, U>(&self, callback: AsyncFn(T) -> U) {
//         self.async_handlers
//             .entry(TypeId::of::<T>())
//             .or_insert(vec![])
//             .push(Box::new(async move |event| {
//                 if let Some(event) = event.downcastast_ref::<E>() {
//                     callback(e).await;
//                 }
//             }));
//     }
// }
