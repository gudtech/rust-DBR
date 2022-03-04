use std::{
    any::{Any, TypeId},
    collections::{btree_map::Entry, BTreeMap, HashMap},
    sync::{Arc, Mutex, RwLock, Weak},
};

use async_trait::async_trait;
use mysql_async::prelude::{Query, WithParams};
//use mysql::prelude::{Queryable, WithParams, BinQuery};

#[derive(Debug)]
pub enum DbrError {
    DowncastError,
    Unimplemented(String),
    PoisonError,
    UnregisteredType,
    RecordNotFetched(i64),
    MissingStore(String),
    MysqlError(mysql::Error),
    MysqlAsyncError(mysql_async::Error),
}

impl From<mysql::Error> for DbrError {
    fn from(err: mysql::Error) -> Self {
        Self::MysqlError(err)
    }
}

impl From<mysql_async::Error> for DbrError {
    fn from(err: mysql_async::Error) -> Self {
        Self::MysqlAsyncError(err)
    }
}

// Implemented on structures that are seen as the working data of the database.
//
// Setting/getting fields from this is essentially like asking the database directly.
pub trait ActiveModel {
    type Model: Send + Sync + Sized + Clone + 'static;
    fn id(&self) -> i64;
    fn data(&self) -> &Arc<Mutex<RecordMetadata<Self::Model>>>;
    fn snapshot(&self) -> Result<Self::Model, DbrError> {
        let locked_record = self.data().lock().map_err(|_| DbrError::PoisonError)?;
        Ok(locked_record.clone().data)
    }
}

pub trait DbrTable {
    type ActiveModel: ActiveModel;
    fn instance_handle() -> &'static str;
    fn table_name() -> &'static str;
}

#[derive(Debug, Clone)]
pub enum InstanceModule {
    Mysql,
    SQLite,
    Unknown(String),
}

pub struct DbrInstances {
    // handle, tag -> dbr instance
    handle_tags: HashMap<(String, Option<String>), DbrInstanceId>,
    instances: HashMap<DbrInstanceId, Arc<DbrInstance>>,
}

impl DbrInstances {
    pub fn new() -> Self {
        Self {
            handle_tags: HashMap::new(),
            instances: HashMap::new(),
        }
    }

    pub fn lookup_by_id(&self, id: DbrInstanceId) -> Option<Arc<DbrInstance>> {
        self.instances.get(&id).cloned()
    }

    pub fn lookup_by_handle(
        &self,
        handle: String,
        tag: Option<String>,
    ) -> Option<Arc<DbrInstance>> {
        let common_instance = self.handle_tags.get(&(handle.clone(), None));
        let instance = self.handle_tags.get(&(handle, tag));
        match (common_instance, instance) {
            (_, Some(id)) => self.lookup_by_id(*id),
            (Some(common_id), None) => self.lookup_by_id(*common_id),
            _ => None,
        }
    }

    pub fn insert(&mut self, instance: DbrInstance) {
        let id = DbrInstanceId(instance.info.id);
        let handle = instance.info.handle.clone();
        let tag = instance.info.tag.clone();

        self.instances.insert(id, Arc::new(instance));
        self.handle_tags.insert((handle, tag), id);
    }
}

#[derive(Debug)]
pub struct DbrInstance {
    pub info: DbrInstanceInfo,
    pub cache: DbrRecordCache,
}

impl DbrInstance {
    pub fn new(info: DbrInstanceInfo) -> Self {
        Self {
            info: info,
            cache: DbrRecordCache::new(),
        }
    }
}

/// Per DBR Instance record cache
///
/// For example, `ops`/`c1` and `ops`/`c2` will have their own record caches.
#[derive(Debug)]
pub struct DbrRecordCache {
    records: RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

pub type Store<T> = BTreeMap<i64, Weak<Mutex<RecordMetadata<T>>>>;

impl std::fmt::Display for DbrError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Self::DowncastError => write!(f, "downcast error"),
            Self::PoisonError => write!(f, "poisoned"),
            Self::UnregisteredType => write!(f, "tried to read unregistered type"),
            Self::RecordNotFetched(id) => write!(f, "record was not available: {}", id),
            Self::Unimplemented(value) => write!(f, "unimplemented {}", value),
            Self::MissingStore(store) => write!(f, "missing store '{}'", store),

            Self::MysqlError(err) => write!(f, "mysql error: {}", err),
            Self::MysqlAsyncError(err) => write!(f, "mysql async error: {}", err),
        }
    }
}

impl std::error::Error for DbrError {}

#[derive(Debug, Clone)]
pub struct RecordMetadata<T> {
    update_time: u64,
    data: T,
}

impl<T> RecordMetadata<T> {
    pub fn new(data: T) -> Self {
        Self {
            update_time: 0,
            data: data,
        }
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

impl<T> ActiveModel for Active<T>
where
    T: Send + Sync + Sized + Clone + 'static,
{
    type Model = T;
    fn id(&self) -> i64 {
        self.id
    }
    fn data(&self) -> &Arc<Mutex<RecordMetadata<T>>> {
        &self.data
    }
}

#[derive(Debug, Clone)]
//#[derive(DbrTable)]
//#[dbr(table = "ops.artist")]
// proc macro attribute to expand out into
pub struct Artist {
    pub id: i64,
    pub name: String,
}

impl DbrTable for Artist {
    type ActiveModel = Active<Artist>;
    fn instance_handle() -> &'static str {
        "ops"
    }
    fn table_name() -> &'static str {
        "artist"
    }
}

pub trait ArtistFields {
    fn name(&self) -> Result<String, DbrError>;
}

impl ArtistFields for Active<Artist> {
    fn name(&self) -> Result<String, DbrError> {
        let snapshot = self.snapshot()?;
        Ok(snapshot.name)
    }
}

/// Global identifier for a DBR instance.
///
/// Equivalent to the id of the dbr.dbr_instances table.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct DbrInstanceId(i64);

#[derive(Debug, Clone)]
pub struct DbrInstanceInfo {
    id: i64,

    /// Database module type, e.g. `Mysql` for mysql/mariadb, `sqlite`, `postgres`
    module: InstanceModule,

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
    pub static ref DBR_INSTANCE_INFO: RwLock<BTreeMap<DbrInstanceId, DbrInstanceInfo>> = RwLock::new(BTreeMap::new());
    pub static ref DBR_INSTANCE_RECORDS: RwLock<BTreeMap<DbrInstanceId, DbrRecordCache>> = RwLock::new(BTreeMap::new());
}

impl DbrInstanceInfo {
    /// Look up all the dbr instances in the metadata database, doesn't necessarily create connections for them.
    pub fn fetch_all(metadata: &mut mysql::Conn) -> Result<Vec<DbrInstanceInfo>, DbrError> {
        use mysql::prelude::Queryable;

        let instances = metadata.query_map(
            r"SELECT instance_id, module, handle, class, tag, dbname, username, password, host, schema_id, dbfile, readonly FROM dbr_instances",
            |(id, module, handle, class, tag, database_name, username, password, host, schema_id, database_file, read_only)| {
                let module: String = module;
                let module = match module.trim().to_lowercase().as_str() {
                    "mysql" => InstanceModule::Mysql,
                    "sqlite" => InstanceModule::SQLite,
                    unknown => InstanceModule::Unknown(unknown.to_owned()),
                };

                DbrInstanceInfo {
                    id, module, handle, class, tag, database_name, username, password, host, schema_id, database_file, read_only,
                }
        })?;
        Ok(instances)
    }

    pub fn connection_uri(&self) -> Option<String> {
        let from = match self.module {
            InstanceModule::Mysql => "mysql",
            InstanceModule::SQLite => "sqlite",
            _ => return None,
        };

        Some(format!(
            "{from}://{user}:{pass}@{host}/{db}",
            from = from,
            user = self.username(),
            pass = self.password(),
            host = self.host(),
            db = self.database_name()
        ))
    }

    pub fn handle(&self) -> &String {
        &self.handle
    }

    pub fn class(&self) -> &String {
        &self.class
    }

    pub fn module(&self) -> &InstanceModule {
        &self.module
    }

    pub fn username(&self) -> &String {
        &self.username
    }

    pub fn password(&self) -> &String {
        &self.password
    }

    pub fn host(&self) -> &String {
        &self.host
    }

    pub fn database_name(&self) -> &String {
        &self.database_name
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

use std::sync::mpsc::{Receiver, Sender};

#[async_trait]
pub trait StoreRequest {
    /*fn run_and_store(&self, conn: &mut mysql_async::Conn, store: Arc<DbrRecordCache>)-> Result<(), DbrError> {
        use futures::executor::block_on;
        block_on(self.run_and_store_async(conn, store))
    }*/
    async fn run_and_store_async(
        &self,
        conn: &mut mysql_async::Conn,
        store: Arc<DbrRecordCache>,
    ) -> Result<(), DbrError>;
}

// TODO HERE: design worker thread that will actually fetch records

// fetch!(&mut conn, Artist where id = 1);
// expands to (minus the fn for type checking/compiling testing purposes)

// on the client side:
fn fetch_client() -> Result<Vec<Active<Artist>>, Box<dyn std::error::Error>> {
    {
        // need to send request to worker thread so lets construct it

        //let artists: Vec<ActiveArtist> = fetch!(&mut context, Artist where id = 1);
        // this expands to
        let artists: Vec<Active<Artist>> = {
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

    /*
    Song {
        id: i64,
        #[relation(Album)]
        album_id: i64,
    }

    Album {
        id: i64,
        #[relation(Artist)]
        artist_id: i64,
    }

    Artist {
        id: i64,
        name: String,
        genre: String,
    } */

    // fetch!(&mut conn, Song where album.artist.genre = "Rock");

    // just some mocking up to lay out some idea on how the processing will work in the macro

    // check if field first
    // if not field check if relational table
    // if relation:
    //     switch context to that relational table
    //     recurse back to checking field for as many relations until a field is found

    pub struct ArtistRequest {
        params: (i64,),
    }

    #[async_trait]
    impl StoreRequest for ArtistRequest {
        async fn run_and_store_async(
            &self,
            conn: &mut mysql_async::Conn,
            store: Arc<DbrRecordCache>,
        ) -> Result<(), DbrError> {
            const QUERY: &'static str = r"SELECT id, name FROM artist WHERE id = ?";
            let results = QUERY
                .with(self.params)
                .map(conn, |(id, name)| Artist { id, name });

            Ok(())
        }
    }

    // SELECT id, album_id FROM song, album, artist WHERE song.album_id = album.id AND album.artist_id = artist.id AND artist.genre = "Rock"
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
