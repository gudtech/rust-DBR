use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex,
    },
};

use crate::prelude::*;

/// Implemented on structures that are seen as the working data of the database.
///
/// Setting/getting fields from this is essentially like asking the database directly.
pub trait ActiveModel<T>
where
    T: Send + Sync + Sized + Clone + 'static,
{
    fn id(&self) -> i64;
    fn data(&self) -> &Arc<Mutex<RecordMetadata<T>>>;
    fn snapshot(&self) -> Result<T, DbrError> {
        let locked_record = self.data().lock().map_err(|_| DbrError::PoisonError)?;
        Ok(locked_record.clone().data)
    }
    fn apply_partial<P: PartialModel<T>>(&self, partial: P) -> Result<(), DbrError> {
        if let Some(id) = partial.id() {
            return Err(DbrError::CannotSetID);
        }

        let mut data = self.data().lock().map_err(|_| DbrError::PoisonError)?;
        partial.apply(&mut *data)?;
        Ok(())
    }
    fn set_snapshot(&self, snapshot: T) -> Result<(), DbrError> {
        let mut data = self.data().lock().map_err(|_| DbrError::PoisonError)?;
        **data = snapshot;
        Ok(())
    }
}

/// Portions of the record to be updated/created.
pub trait PartialModel<T> {
    fn apply<R>(self, record: &mut R) -> Result<(), DbrError>
    where
        R: Deref<Target = T> + DerefMut;
    fn id(&self) -> Option<i64>;
}

#[derive(Debug)]
pub struct Active<T> {
    id: i64,
    data: Arc<Mutex<RecordMetadata<T>>>,
}

impl<T> Active<T> {
    pub fn from_arc(id: i64, data: Arc<Mutex<RecordMetadata<T>>>) -> Self {
        Self { id, data }
    }
}

impl<T> ActiveModel<T> for Active<T>
where
    T: Send + Sync + Sized + Clone + 'static,
{
    fn id(&self) -> i64 {
        self.id
    }
    fn data(&self) -> &Arc<Mutex<RecordMetadata<T>>> {
        &self.data
    }
}
