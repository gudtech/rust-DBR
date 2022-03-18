use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

use crate::prelude::*;

/// Implemented on structures that are seen as the working data of the database.
///
/// Setting/getting fields from this is essentially like asking the database directly.
pub trait ActiveModel<T>
where
    T: DbrTable + Send + Sync + Sized + Clone + 'static,
{
    fn id(&self) -> <T as DbrTable>::Id;
    fn data(&self) -> &Arc<Mutex<RecordMetadata<T>>>;
    fn snapshot(&self) -> Result<T, DbrError> {
        let locked_record = self.data().lock().map_err(|_| DbrError::PoisonError)?;
        Ok(locked_record.clone().data)
    }
    fn apply_partial<P: PartialModel<T>>(&self, partial: P) -> Result<(), DbrError> {
        if let Some(_id) = partial.id() {
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
pub trait PartialModel<T>
where
    T: DbrTable,
{
    fn apply<R>(self, record: &mut R) -> Result<(), DbrError>
    where
        R: Deref<Target = T> + DerefMut;
    fn id(&self) -> Option<<T as DbrTable>::Id>;
}

#[derive(Debug, Clone)]
pub struct Active<T>
where
    T: DbrTable,
{
    id: <T as DbrTable>::Id,
    data: Arc<Mutex<RecordMetadata<T>>>,
}

impl<T> Active<T>
where
    T: DbrTable,
{
    pub fn from_arc(id: <T as DbrTable>::Id, data: Arc<Mutex<RecordMetadata<T>>>) -> Self {
        Self { id, data }
    }
}

impl<T> ActiveModel<T> for Active<T>
where
    T: DbrTable,
{
    fn id(&self) -> <T as DbrTable>::Id {
        self.id.clone()
    }
    fn data(&self) -> &Arc<Mutex<RecordMetadata<T>>> {
        &self.data
    }
}
