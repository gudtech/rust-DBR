use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};

use sqlx::{Decode, FromRow, MySql};

use crate::prelude::*;

/// Global identifier for a DBR instance.
///
/// Equivalent to the id of the dbr.dbr_instances table.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct DbrInstanceId(pub u32);

#[derive(FromRow, Debug, Clone)]
pub struct DbrInstanceInfo {
    #[sqlx(rename = "instance_id")]
    id: u32,

    /// Database module type, e.g. `MySql`, `sqlite`, `postgres`
    module: String,

    /// Instance schema, e.g. `config`/`ops`/`constants`/`directory`
    #[sqlx(rename = "handle")]
    schema: String,

    /// Type of instance, currently just `master` by default or `template` for template instance
    class: String,

    /// Tag of the instance, currently just used in our purposes to distinguish between client instances
    tag: Option<String>,

    /// Parameters on connecting to the database
    #[sqlx(rename = "dbname")]
    database_name: String,
    username: String,
    password: String,
    host: String,

    /// Extraneous fields here for the sake of modeling the dbr.dbr_instances table.
    ///
    /// Could be useful for something in the future, but I'm not entirely sure yet.
    ///
    /// Feel free to move them above and add a comment if you think otherwise!
    schema_id: i32,
    #[sqlx(rename = "dbfile")]
    database_file: Option<String>,
    #[sqlx(rename = "readonly")]
    read_only: Option<bool>,
}

lazy_static::lazy_static! {
    pub static ref DBR_INSTANCE_INFO: RwLock<BTreeMap<DbrInstanceId, DbrInstanceInfo>> = RwLock::new(BTreeMap::new());
    pub static ref DBR_INSTANCE_RECORDS: RwLock<BTreeMap<DbrInstanceId, DbrRecordCache>> = RwLock::new(BTreeMap::new());
}

impl DbrInstanceInfo {
    /// Look up all the dbr instances in the metadata database, doesn't necessarily create connections for them.
    pub async fn fetch_all<'c, E: sqlx::Executor<'c, Database = MySql>>(
        executor: E,
    ) -> Result<Vec<DbrInstanceInfo>, DbrError> {
        let instances = sqlx::query_as(r"SELECT instance_id, module, handle, class, tag, dbname, username, password, host, schema_id, dbfile, readonly FROM dbr_instances")
            .fetch_all(executor).await?;

        Ok(instances)
    }

    pub fn connection_host_uri(&self) -> String {
        format!(
            "{from}://{user}:{pass}@{host}/",
            from = self.module(),
            user = self.username(),
            pass = self.password(),
            host = self.host(),
        )
    }

    pub fn connection_uri(&self) -> String {
        format!(
            "{uri}/{db}",
            uri = self.connection_host_uri(),
            db = self.database_name()
        )
    }

    // Are these a part of the same database?
    //
    // We don't include the "schema" here because you can have cases like
    // constants and directory being in the same database but a different schema.
    //
    // Probaby should check if this is fine.
    pub fn colocated<O: Borrow<Self>>(&self, other: O) -> bool {
        self.connection_host_uri() == other.borrow().connection_host_uri()
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn schema(&self) -> &String {
        &self.schema
    }

    pub fn class(&self) -> &String {
        &self.class
    }

    /*
    pub fn module(&self) -> &InstanceModule {
        &self.module
    } */
    pub fn module(&self) -> String {
        self.module.to_owned().to_lowercase()
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

    pub fn set_host(&mut self, new_host: String) {
        self.host = new_host;
    }

    pub fn database_name(&self) -> &String {
        &self.database_name
    }

    pub fn tag(&self) -> &Option<String> {
        &self.tag
    }
}

#[derive(sqlx::Type, Debug, Clone)]
#[sqlx(type_name = "VARCHAR")]
#[sqlx(rename_all = "lowercase")]
pub enum InstanceModule {
    MySql,
    SQLite,
    Postgres,
}

#[derive(Debug, Clone)]
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

    pub fn lookup_by_id(&self, id: DbrInstanceId) -> Result<Arc<DbrInstance>, DbrError> {
        self.instances
            .get(&id)
            .cloned()
            .ok_or(DbrError::MissingInstance {
                id: Some(id),
                handle: None,
                tag: None,
            })
    }

    pub fn lookup_by_handle(
        &self,
        handle: String,
        tag: Option<String>,
    ) -> Result<Arc<DbrInstance>, DbrError> {
        let common_instance = self.handle_tags.get(&(handle.clone(), None));
        let instance = self.handle_tags.get(&(handle.clone(), tag.clone()));
        let mut result = match (common_instance, instance) {
            (_, Some(id)) => self.lookup_by_id(*id),
            (Some(common_id), None) => self.lookup_by_id(*common_id),
            _ => Err(DbrError::MissingInstance {
                id: None,
                handle: None,
                tag: None,
            }),
        };

        if let Err(DbrError::MissingInstance {
            id: err_id,
            handle: err_handle,
            tag: err_tag,
        }) = &mut result
        {
            *err_handle = Some(handle);
            *err_tag = tag;
        }

        result
    }

    pub fn insert(&mut self, instance: DbrInstance) {
        let id = DbrInstanceId(instance.info.id());
        let handle = instance.info.schema().clone();
        let tag = instance.info.tag().clone();

        self.instances.insert(id, Arc::new(instance));
        self.handle_tags.insert((handle, tag), id);
    }
}

#[derive(Debug)]
pub enum Pool {
    MySql(sqlx::Pool<sqlx::MySql>),
    Sqlite(sqlx::Pool<sqlx::Sqlite>),
}

#[derive(Debug)]
pub struct DbrInstance {
    pub info: DbrInstanceInfo,
    pub cache: DbrRecordCache,
    pub pool: Pool,
}

impl DbrInstance {
    pub async fn new(info: DbrInstanceInfo) -> Result<Self, DbrError> {
        let uri = info.connection_uri();
        dbg!(&uri);
        let pool = match info.module().as_str() {
            "mysql" => {
                let pool = sqlx::Pool::<sqlx::MySql>::connect(&uri).await?;
                Pool::MySql(pool)
            }
            "sqlite" => {
                let pool = sqlx::Pool::<sqlx::Sqlite>::connect(&uri).await?;
                Pool::Sqlite(pool)
            }
            _ => return Err(DbrError::PoolDisconnected),
        };

        Ok(Self {
            info: info,
            cache: DbrRecordCache::new(),
            pool: pool,
        })
    }
}
