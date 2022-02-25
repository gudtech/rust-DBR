

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

use std::{sync::{Arc, Mutex}, collections::HashMap, ops::DerefMut};

use mysql::Error;

pub trait Queryable
where
    Self: Sized
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

    fn fetch_raw<C: DerefMut<Target = mysql::Conn>>(conn: C, conditions: Vec<Condition>) -> Result<Vec<mysql::Row>, Error>;

    fn from_row(callsite: DbrCallsite, row: mysql::Row) -> Result<Self, Error>;
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
    () => {
        {
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
        */
    }
}

//#[derive(DBR, Default)]
//#[relation(Album, remotekey = "artist_id")]
pub struct Artist {
    pub _meta: DbrCallsite,
    pub id: i64,
    pub name: Option<String>,
}

// proc macro to expand out into
impl Queryable for Artist {
    fn fetch_raw<C: DerefMut<Target = mysql::Conn>>(conn: C, conditions: Vec<Condition>) -> Result<Vec<mysql::Row>, Error> {
        Ok(Vec::new())
    }

    fn from_row(object: DbrCallsite, row: mysql::Row) -> Result<Self, Error> {
        let columns = row.columns_ref();
        let mut artist =  Self::default_with_object(object);

        for (index, column) in columns.iter().enumerate() {
            match column.name_str().to_string().as_str() {
                "id" => artist.id = row.get(index).expect("need id for dbr object"),
                "name" => artist.name = row.get(index),
                _ => {},
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
                let name: Option<String> = meta.connection.lock()?.query_first("SELECT name FROM artist WHERE id = :id")?;
                meta.extra_fields_queried.push("name".to_owned());
                *value = name;
                Ok(value.as_ref().unwrap())
            }
        }
    }
}
