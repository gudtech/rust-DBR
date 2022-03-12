use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};

use sqlx::{Decode, FromRow, MySql};

use crate::prelude::*;

/// Global identifier for a DBR instance.
///
/// Equivalent to the id of the dbr.dbr_instances table.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct DbrInstanceId(pub i64);

#[derive(FromRow, Debug, Clone)]
pub struct DbrInstanceInfo {
    id: i64,

    /// Database module type, e.g. `Mysql` for mysql/mariadb, `sqlite`, `postgres`
    module: InstanceModule,

    /// Instance schema, e.g. `config`/`ops`/`constants`/`directory`
    schema: String,

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
    pub async fn fetch_all<'c, E: sqlx::Executor<'c, Database = MySql>>(
        executor: E,
    ) -> Result<Vec<DbrInstanceInfo>, DbrError> {
        let instances = sqlx::query_as(r"SELECT instance_id, module, handle, class, tag, dbname, username, password, host, schema_id, dbfile, readonly FROM dbr_instances")
            .fetch_all(executor).await?;

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

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn schema(&self) -> &String {
        &self.schema
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

    pub fn tag(&self) -> &Option<String> {
        &self.tag
    }
}

#[derive(sqlx::Type, Debug, Clone)]
#[sqlx(type_name = "color")] // only for PostgreSQL to match a type definition
#[sqlx(rename_all = "lowercase")]
pub enum InstanceModule {
    Mysql,
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
    Disconnected,
}

#[derive(Debug)]
pub struct DbrInstance {
    pub info: DbrInstanceInfo,
    pub cache: DbrRecordCache,
    pub pool: Pool,
}

impl DbrInstance {
    pub fn new(info: DbrInstanceInfo) -> Self {
        Self {
            info: info,
            cache: DbrRecordCache::new(),
            pool: Pool::Disconnected,
        }
    }
}
