
use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, RwLock, Weak},
};

pub trait Queryable
where
    Self: Sized,
{
    type Snapshot;
    fn id(&self) -> i64;
    fn snapshot(&self) -> Result<Self::Snapshot, DbrError>;
}

pub struct DbrRecordStore {
    records: RwLock<HashMap<TypeId, Box<dyn Any>>>,
}

type Store<T> = BTreeMap<i64, Weak<Mutex<T>>>;

#[derive(Debug)]
pub enum DbrError {
    DowncastError,
    Unimplemented(String),
    PoisonError,
    UnregisteredType,
    RecordNotFetched(i64),
}

impl std::fmt::Display for DbrError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Self::DowncastError => write!(f, "downcast error"),
            Self::PoisonError => write!(f, "poisoned"),
            Self::UnregisteredType => write!(f, "tried to read unregistered type"),
            Self::RecordNotFetched(id) => write!(f, "record was not available: {}", id),
            Self::Unimplemented(value) => write!(f, "unimplemented {}", value),
        }
    }
}

impl std::error::Error for DbrError {}

impl DbrRecordStore {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
        }
    }

    pub fn register<T: Any>(&self) -> Result<(), DbrError> {
        let mut map = self.records.write().map_err(|_| DbrError::PoisonError)?;
        map.entry(TypeId::of::<T>())
            .or_insert(Box::new(Store::<T>::new()));
        Ok(())
    }

    pub fn is_registered<T: Any>(&self) -> Result<bool, DbrError> {
        let map = self.records.read().map_err(|_| DbrError::PoisonError)?;
        let contains = map.contains_key(&TypeId::of::<T>());
        Ok(contains)
    }

    pub fn assert_registered<T: Any>(&self) -> Result<(), DbrError> {
        if !self.is_registered::<T>()? {
            self.register::<T>()?;
        }

        Ok(())
    }

    pub fn record<T: Any>(&self, id: i64) -> Result<Arc<Mutex<T>>, DbrError> {
        self.assert_registered::<T>()?;

        let map = self.records.read().map_err(|_| DbrError::PoisonError)?;
        match map.get(&TypeId::of::<T>()) {
            Some(records) => {
                match records.downcast_ref::<Store<T>>() {
                    Some(downcasted) => {
                        if let Some(record) = downcasted.get(&id) {
                            if let Some(strong) = record.upgrade() {
                                return Ok(strong);
                            }
                        }

                        Err(DbrError::RecordNotFetched(id))
                    }
                    None => Err(DbrError::DowncastError),
                }
            }
            None => Err(DbrError::UnregisteredType),
        }
    }
}

//#[dbr(table_name = "artist")]
//#[relation(Album, remotekey = "artist_id")]
pub struct Artist {
    pub id: i64,
    pub store: Arc<DbrRecordStore>,
}

// proc macro attribute to expand out into
#[derive(Debug, Clone)]
pub struct ArtistSnapshot {
    pub id: i64,
    pub name: String,
}

impl Queryable for Artist {
    type Snapshot = ArtistSnapshot;
    fn id(&self) -> i64 {
        self.id
    }

    fn snapshot(&self) -> Result<ArtistSnapshot, DbrError> {
        let record = self.store.record::<ArtistSnapshot>(self.id())?;
        let locked_record = record.lock().map_err(|_| DbrError::PoisonError)?;
        Ok(locked_record.clone())
    }
}

impl Artist {
    pub fn name(&self) -> Result<String, DbrError> {
        let snapshot = self.snapshot()?;
        Ok(snapshot.name)
    }
}