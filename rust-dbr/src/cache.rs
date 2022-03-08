
use std::{ops::{Deref, DerefMut}, any::{Any, TypeId}, sync::{RwLock, Arc, Weak, Mutex}, collections::{BTreeMap, HashMap, btree_map::Entry}};
use crate::prelude::*;

pub type Store<T> = BTreeMap<i64, Weak<Mutex<RecordMetadata<T>>>>;

/// Per DBR Instance record cache
///
/// For example, `ops`/`c1` and `ops`/`c2` will have their own record caches.
#[derive(Debug)]
pub struct DbrRecordCache {
    records: RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

#[derive(Debug, Clone)]
pub struct RecordMetadata<T> {
    pub update_time: u64,
    pub data: T,
}

impl<T> RecordMetadata<T> {
    pub fn new(data: T) -> Self {
        Self {
            update_time: 0,
            data: data,
        }
    }
}

impl<T> Deref for RecordMetadata<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for RecordMetadata<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl DbrRecordCache {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
        }
    }

    pub fn register<T: Any + Send + Sync>(&self) -> Result<(), DbrError> {
        let mut map = self.records.write().map_err(|_| DbrError::PoisonError)?;
        map.entry(TypeId::of::<T>())
            .or_insert(Box::new(Store::<T>::new()));
        Ok(())
    }

    pub fn is_registered<T: Any + Send + Sync>(&self) -> Result<bool, DbrError> {
        let map = self.records.read().map_err(|_| DbrError::PoisonError)?;
        let contains = map.contains_key(&TypeId::of::<T>());
        Ok(contains)
    }

    pub fn assert_registered<T: Any + Send + Sync>(&self) -> Result<(), DbrError> {
        if !self.is_registered::<T>()? {
            self.register::<T>()?;
        }

        Ok(())
    }

    pub fn set_record<T: Any + Send + Sync>(
        &self,
        id: i64,
        record: T,
    ) -> Result<Arc<Mutex<RecordMetadata<T>>>, DbrError> {
        self.assert_registered::<T>()?;

        let mut map = self.records.write().map_err(|_| DbrError::PoisonError)?;
        match map.get_mut(&TypeId::of::<T>()) {
            Some(records) => match records.downcast_mut::<Store<T>>() {
                Some(downcasted) => {
                    match downcasted.entry(id) {
                        Entry::Occupied(mut occupied) => {
                            match occupied.get().upgrade() {
                                Some(strong) => {
                                    {
                                        let mut locked_existing =
                                            strong.lock().map_err(|_| DbrError::PoisonError)?;
                                        *locked_existing = RecordMetadata::new(record);
                                    }

                                    Ok(strong)
                                }
                                None => {
                                    // record doesn't actually exist anymore lets go make a new one.
                                    let strong = Arc::new(Mutex::new(RecordMetadata::new(record)));
                                    let weak = Arc::downgrade(&strong);
                                    occupied.insert(weak);
                                    Ok(strong)
                                }
                            }
                        }
                        Entry::Vacant(vacant) => {
                            let strong = Arc::new(Mutex::new(RecordMetadata::new(record)));
                            let weak = Arc::downgrade(&strong);
                            vacant.insert(weak);
                            Ok(strong)
                        }
                    }
                }
                None => Err(DbrError::DowncastError),
            },
            None => Err(DbrError::UnregisteredType),
        }
    }

    pub fn record<T: Any + Send + Sync>(
        &self,
        id: i64,
    ) -> Result<Arc<Mutex<RecordMetadata<T>>>, DbrError> {
        self.assert_registered::<T>()?;

        let map = self.records.read().map_err(|_| DbrError::PoisonError)?;
        match map.get(&TypeId::of::<T>()) {
            Some(records) => match records.downcast_ref::<Store<T>>() {
                Some(downcasted) => {
                    if let Some(record) = downcasted.get(&id) {
                        if let Some(strong) = record.upgrade() {
                            return Ok(strong);
                        }
                    }

                    Err(DbrError::RecordNotFetched(id))
                }
                None => Err(DbrError::DowncastError),
            },
            None => Err(DbrError::UnregisteredType),
        }
    }
}
