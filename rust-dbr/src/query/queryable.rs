pub struct Condition {
    field: String,
    value: Option<String>,
}

pub trait ToSql {
    fn to_sql(&self) -> String;
}

impl ToSql for Condition {
    fn to_sql(&self) -> String {
        match self.value.clone() {
            Some(value) => format!("{field} = {value}", field = self.field, value = value),
            None => format!("{field} IS NULL", field = self.field),
        }
    }
}

use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashMap},
    ops::DerefMut,
    sync::{Arc, Mutex, Weak},
};

use mysql::Error;

pub trait Queryable
where
    Self: Sized,
{
    fn fetch(callsite: DbrCallsite, conditions: Vec<Condition>) -> Result<Vec<Self>, Error> {
        let rows = Self::fetch_raw(callsite.meta.lock()?.connection.lock()?, conditions)?;

        let mut objects = Vec::new();
        for row in rows {
            let callsite = callsite.duplicate_callsite();
            objects.push(Self::from_row(callsite, row)?);
        }

        Ok(objects)
    }

    fn all(callsite: DbrCallsite) -> Result<Vec<Self>, Error> {
        Self::fetch(callsite, Vec::new())
    }

    fn fetch_raw<C: DerefMut<Target = mysql::Conn>>(
        conn: C,
        conditions: Vec<Condition>,
    ) -> Result<Vec<mysql::Row>, Error>;

    fn from_row(callsite: DbrCallsite, row: mysql::Row) -> Result<Self, Error>;
}

pub struct DbrRecordStore {
    records: HashMap<TypeId, Box<dyn Any>>,
}

type Store<T> = BTreeMap<i64, Weak<Mutex<T>>>;

#[derive(Debug)]
pub enum DbrError {
    DowncastError,
    Unimplemented(String),
}

impl std::fmt::Display for DbrError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Self::DowncastError => write!(f, "downcast error"),
            Self::Unimplemented(value) => write!(f, "unimplemented {}", value),
        }
    }
}

impl std::error::Error for DbrError {}

impl DbrRecordStore {
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
        }
    }

    pub fn register<T: Any>(&mut self) {
        let records = self
            .records
            .entry(TypeId::of::<T>())
            .or_insert(Box::new(Store::<T>::new()));
    }

    pub fn record<T: Any>(&self, id: i64) -> Result<Arc<Mutex<T>>, Box<dyn std::error::Error>> {
        match self.records.get(&TypeId::of::<T>()) {
            Some(records) => {
                match records.downcast_ref::<Store<T>>() {
                    Some(downcasted) => {
                        if let Some(record) = downcasted.get(&id) {
                            if let Some(strong) = record.upgrade() {
                                return Ok(strong);
                            }
                        }

                        // we need to go fetch the record now
                        // either we didn't have the record already, or the one we had just died.

                        Err(Box::new(DbrError::Unimplemented("fetch record".to_owned())))
                    }
                    None => Err(Box::new(DbrError::Unimplemented("downcast err".to_owned()))),
                }
            }
            None => Err(Box::new(DbrError::Unimplemented(
                "unregistered type".to_owned(),
            ))),
        }
    }
}

pub struct CallsiteCache {
    callsites: HashMap<u128, Arc<Mutex<Vec<String>>>>,
}

pub struct DbrCallsite {
    pub meta: Arc<Mutex<DbrMeta>>,
}

impl DbrCallsite {
    pub fn duplicate_callsite(&self) -> Self {
        Self {
            meta: self.meta.clone(),
        }
    }
}

pub struct DbrMeta {
    // some hash of the callsite so we can remember what was fetched
    pub callsite: u64,
    pub callsite_cache: Arc<Mutex<Vec<String>>>,
    pub connection: Arc<Mutex<mysql::Conn>>,
    pub extra_fields_queried: Vec<String>,
}

impl Drop for DbrMeta {
    fn drop(&mut self) {
        if self.extra_fields_queried.len() > 0 {
            if let Ok(mut cache) = self.callsite_cache.lock() {
                cache.extend(self.extra_fields_queried.clone());
            }
        }
    }
}

#[macro_export]
macro_rules! callsite {
    () => {{
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        file!().hash(&mut hasher);
        line!().hash(&mut hasher);
        column!().hash(&mut hasher);
        let hash = hasher.finish();
        dbg!(hash);
    }
    /*
    DbrObject {
        callsite: hash,
        connection: conn,
        callsite_cache:
    }
    */};
}

//#[derive(DBR, Default)]
//#[relation(Album, remotekey = "artist_id")]
pub struct Artist {
    pub id: i64,
    pub store: Arc<DbrRecordStore>,
}

// proc macro to expand out into
#[derive(Debug, Clone)]
pub struct ArtistSnapshot {
    pub id: i64,
    pub name: String,
}

impl Artist {
    pub fn snapshot(&self) -> Result<ArtistSnapshot, Box<dyn std::error::Error>> {
        let record = self.store.record::<ArtistSnapshot>(self.id())?;
        let locked = record.lock();
        match locked {
            Ok(locked_record) => Ok(locked_record.clone()),
            Err(_) => Err(Box::new(DbrError::Unimplemented("poisoned".to_owned())))
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn name(&self) -> Result<String, Box<dyn std::error::Error>> {
        let snapshot = self.snapshot()?;
        Ok(snapshot.name)
    }
}

/*
impl Queryable for Artist {
    fn fetch_raw<C: DerefMut<Target = mysql::Conn>>(
        conn: C,
        conditions: Vec<Condition>,
    ) -> Result<Vec<mysql::Row>, Error> {
        Ok(Vec::new())
    }

    fn from_row(object: DbrCallsite, row: mysql::Row) -> Result<Self, Error> {
        let columns = row.columns_ref();
        let mut artist = Self::default_with_object(object);

        for (index, column) in columns.iter().enumerate() {
            match column.name_str().to_string().as_str() {
                "id" => artist.id = row.get(index).expect("need id for dbr object"),
                //"name" => artist.name = row.get(index),
                _ => {}
            }
        }

        Ok(artist)
    }
}

impl Artist {
    pub fn default_with_object(object: DbrCallsite) -> Self {
        Self {
            _meta: object,
            id: 0,
            name: None,
        }
    }

    fn id(&self) -> i64 {
        self.id
    }

    fn name(&mut self) -> Result<&String, Error> {
        match &mut self.name {
            Some(name) => Ok(&*name),
            value => {
                use mysql::prelude::Queryable;
                let mut meta = self._meta.meta.lock()?;
                // ok we need to go fetch it and add this to the cache
                let name: Option<String> = meta
                    .connection
                    .lock()?
                    .query_first("SELECT name FROM artist WHERE id = :id")?;
                meta.extra_fields_queried.push("name".to_owned());
                *value = name;
                Ok(value.as_ref().unwrap())
            }
        }
    }
}
*/
