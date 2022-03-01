use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, RwLock, Weak},
};

use mysql::prelude::Queryable;

#[derive(Debug)]
pub enum DbrError {
    DowncastError,
    Unimplemented(String),
    PoisonError,
    UnregisteredType,
    RecordNotFetched(i64),
    MysqlError(mysql::Error),
}

impl From<mysql::Error> for DbrError {
    fn from(err: mysql::Error) -> Self {
        Self::MysqlError(err)
    }
}

// Implemented on structures that are seen as the working data of the database.
//
// Setting/getting fields from this is essentially like asking the database directly.
pub trait ActiveModel {
    type Model: Send + Sync + Sized + Clone + 'static;
    fn id(&self) -> i64;
    fn store(&self) -> &Arc<DbrRecordStore>;
    fn model(&self) -> Result<Self::Model, DbrError> {
        let record = self.store().record::<Self::Model>(self.id())?;
        let locked_record = record.lock().map_err(|_| DbrError::PoisonError)?;
        Ok(locked_record.clone())
    }
}

pub struct DbrRecordStore {
    records: RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

pub type Store<T> = BTreeMap<i64, Weak<Mutex<T>>>;

impl std::fmt::Display for DbrError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Self::DowncastError => write!(f, "downcast error"),
            Self::PoisonError => write!(f, "poisoned"),
            Self::UnregisteredType => write!(f, "tried to read unregistered type"),
            Self::RecordNotFetched(id) => write!(f, "record was not available: {}", id),
            Self::Unimplemented(value) => write!(f, "unimplemented {}", value),

            Self::MysqlError(err) => write!(f, "mysql error: {}", err),
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

    pub fn record<T: Any + Send + Sync>(&self, id: i64) -> Result<Arc<Mutex<T>>, DbrError> {
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

//#[dbr(table_name = "artist")]
//#[derive(DbrTable)]
//#[dbr(table_name = "artist")]
//#[relation(Album, remotekey = "artist_id")]
// proc macro attribute to expand out into
#[derive(Debug, Clone)]
//#[dbr(table_name = "artist")]
pub struct Artist {
    pub id: i64,
    pub name: String,
}

pub struct ActiveArtist {
    id: i64,
    store: Arc<DbrRecordStore>,
}

impl ActiveModel for ActiveArtist {
    type Model = Artist;
    fn id(&self) -> i64 {
        self.id
    }
    fn store(&self) -> &Arc<DbrRecordStore> {
        &self.store
    }
}

impl ActiveArtist {
    pub fn name(&self) -> Result<String, DbrError> {
        let model = self.model()?;
        Ok(model.name)
    }
}

/// Global identifier for a DBR instance.
///
/// Equivalent to the id of the dbr.dbr_instances table.
#[derive(Debug, Copy, Clone)]
pub struct DbrInstanceId(i64);

#[derive(Debug, Clone)]
pub struct DbrInstance {
    id: i64,

    /// Database module type, e.g. `Mysql` for mysql/mariadb, `sqlite`, `postgres`
    module: String,

    /// Instance handle, e.g. `config`/`ops`/`constants`/`directory`
    handle: String,

    /// Type of instance, currently just `master` by default or `template` for template instance
    class: String,

    /// Tag of the instance, currently just used in our purposes to distinguish between client instances
    tag: Option<String>,

    /// Parameters on connecting to the database
    database_name: String,
    username: String,
    password: String,
    host: String,

    /// Extraneous fields here for the sake of modeling the dbr.dbr_instances table.
    ///
    /// Could be useful for something in the future, but I'm not entirely sure yet.
    ///
    /// Feel free to move them above and add a comment if you think otherwise!
    schema_id: i64,
    database_file: Option<String>,
    read_only: Option<bool>,
}

lazy_static::lazy_static! {
    pub static ref DBR_INSTANCE_INFO: RwLock<BTreeMap<DbrInstanceId, DbrInstance>> = RwLock::new(BTreeMap::new());
    pub static ref DBR_INSTANCE_RECORDS: RwLock<BTreeMap<DbrInstanceId, DbrRecordStore>> = RwLock::new(BTreeMap::new());
}

impl DbrInstance {
    /// Look up all the dbr instances in the metadata database, doesn't necessarily create connections for them.
    pub fn fetch_all(metadata: &mut mysql::Conn) -> Result<Vec<DbrInstance>, DbrError> {
        let instances = metadata.query_map(
            r"SELECT instance_id, module, handle, class, tag, dbname, username, password, host, schema_id, dbfile, readonly FROM dbr_instances",
            |(id, module, handle, class, tag, database_name, username, password, host, schema_id, database_file, read_only)| {
                DbrInstance {
                    id, module, handle, class, tag, database_name, username, password, host, schema_id, database_file, read_only,
                }
        })?;
        Ok(instances)
    }

    pub fn common_instances<'a>(instances: impl Iterator<Item = &'a DbrInstance>) -> Vec<DbrInstanceId> {
        instances
            .filter(|instance| instance.tag.is_none() && instance.class == "master")
            .map(|instance| DbrInstanceId(instance.id))
            .collect()
    }

    pub fn client_instances<'a>(
        client_id: i64,
        instances: impl Iterator<Item = &'a DbrInstance>,
    ) -> Vec<DbrInstanceId> {
        let client_tag = format!("c{}", client_id);
        instances
            .filter(|instance| {
                instance.tag.is_some() // we check for some right before so this should be fine.
                    && instance.tag.as_ref().unwrap() == &client_tag
                    && instance.class == "master"
            })
            .map(|instance| DbrInstanceId(instance.id))
            .collect()
    }
}

pub struct DbrContext {
    pub client_id: i64,
    pub instances: Vec<DbrInstanceId>,
}

impl DbrContext {
    pub fn from_client_id(client_id: i64) -> Self {
        Self {
            client_id: client_id,
            instances: Vec::new(),
        }
    }
}

// fetch!(&mut conn, Artist where id = 1);
// expands to (minus the fn for type checking/compiling testing purposes)

// on the client side:
fn fetch_client() -> Result<Vec<ActiveArtist>, Box<dyn std::error::Error>> {
    {
        // need to send request to worker thread so lets construct it

        //let artists: Vec<ActiveArtist> = fetch!(&mut context, Artist where id = 1);
        // this expands to
        let artists: Vec<ActiveArtist> = {
            async {};
            Vec::new()
        };
        Ok(artists)
    }
}

// on the dbr record store worker thread side:
fn fetch_record_store() -> Result<Vec<Artist>, Box<dyn std::error::Error>> {
    let opts = mysql::Opts::from_url("mysql://devuser:password@localhost:3306/")?;

    let mut conn = mysql::Conn::new(opts)?;
    let id: i64 = 1;

    {
        use mysql::prelude::Queryable;
        const QUERY: &'static str = r"SELECT id, name FROM artist WHERE id = ?";
        if let Ok(result_set) = conn.exec(QUERY, (id,)) {
            let mut results = Vec::new();
            for (id, name) in result_set {
                results.push(Artist { id, name });
            }

            Ok(results)
        } else {
            Err(Box::new(DbrError::Unimplemented(
                "could not convert to artist snapshot".to_owned(),
            )))
        }
    }
}
