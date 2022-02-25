

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

use std::sync::Arc;

use mysql::Error;

pub trait Queryable
where
    Self: Sized
{
    fn fetch(conditions: Vec<Condition>) -> Result<Vec<Self>, Error> {
        let rows = Self::fetch_raw(conditions)?;

        let mut objects = Vec::new();
        for row in rows {
            objects.push(Self::from_row(row)?);
        }

        Ok(objects)
    }

    fn all() -> Result<Vec<Self>, Error> {
        Self::fetch(Vec::new())
    }

    fn fetch_raw(conditions: Vec<Condition>) -> Result<Vec<mysql::Row>, Error>;

    fn from_row(row: mysql::Row) -> Result<Self, Error>;
}


pub struct DbrObject<T> {
    // some hash of the callsite so we can remember what was fetched
    callsite: u128,
    connection: Arc<mysql::Conn>,
    inner: T,
}

//#[derive(DBR, Default)]
//#[relation(Album, remotekey = "artist_id")]
#[derive(Default)]
pub struct Artist {
    pub id: Option<i64>,
    pub name: Option<String>,
}

// proc macro to expand out into
impl Queryable for Artist {
    fn fetch_raw(conditions: Vec<Condition>) -> Result<Vec<mysql::Row>, Error> {
        Ok(Vec::new())
    }

    fn from_row(row: mysql::Row) -> Result<Self, Error> {
        let columns = row.columns_ref();
        let mut artist =  Self::default();

        for (index, column) in columns.iter().enumerate() {
            match column.name_str().to_string().as_str() {
                "id" => artist.id = row.get(index),
                "name" => artist.name = row.get(index),
                _ => {},
            }
        }

        Ok(artist)
    }
}

pub trait ArtistFetch {
    fn id(&mut self) -> Result<&i64, Error>;
    fn name(&mut self) -> Result<&String, Error>;
}

impl ArtistFetch for DbrObject<Artist> {
    fn id(&mut self) -> Result<&i64, Error> {
        match &self.inner.id {
            Some(id) => Ok(id),
            None => {
                unimplemented!()
            }
        }
    }

    fn name(&mut self) -> Result<&String, Error> {
        match &self.inner.name {
            Some(name) => Ok(name),
            None => {
                // ok we need to go fetch it and add this to the cache
                //self.connection.query(...)
                unimplemented!()
            }
        }
    }
}
