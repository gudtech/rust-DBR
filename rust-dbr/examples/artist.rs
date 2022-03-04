//use rust_dbr::query::queryable::DbrObject;

//#[dbr(table_name = "artist")]
#[derive(Debug)]
pub struct Artist {
    id: i64,
    name: String,
}

//#[dbr(table_name = "album")]
#[derive(Debug)]
pub struct Album {
    id: i64,
    artist_id: i64,
    name: String,
    date_released: u64,
}

pub struct Context {
    //client: Client,
    conn: mysql::Conn,
}

pub struct Request {
    request_id: i64,
}

pub struct Response {}

/*
pub fn get_albums(context: Context, params: RequestParams) -> Result<Response, Error> {
    let artists = Artist::fetch_all();

    artist::fetch!();

    for artist in artists {
        let name = artist.name()?;
        dbg!(&name);

        let albums = artist.albums()?;
        dbg!(&albums);

        for album in albums {
            let released = album.date_released()?;
            dbg!(&released);
        }
    }

    Ok(Response { })
}
*/

use std::{collections::HashMap, sync::Arc};

use futures::future::BoxFuture;
use mysql_async::prelude::*;
use rust_dbr::query::queryable::{
    Active, DbrError, DbrInstance, DbrInstances, DbrRecordCache, DbrTable, DbrInstanceInfo,
};
#[derive(Debug, PartialEq, Eq, Clone)]
struct Payment {
    customer_id: i32,
    amount: i32,
    account_name: Option<String>,
}

pub struct FetchRequest<'a, T> {
    future: BoxFuture<'a, Result<Vec<T>, mysql_async::Error>>,
}

pub struct FetchSingleRequest<'a, T> {
    future: BoxFuture<'a, Result<T, mysql_async::Error>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = "mysql://devuser:password@localhost:3306/account_test";

    let pool = mysql_async::Pool::new(database_url);
    let mut conn = pool.get_conn().await?;

    #[derive(Debug, Clone)]
    pub struct Song {
        id: i64,
        name: String,
        album_id: i64,
    }

    impl DbrTable for Song {
        type ActiveModel = Active<Song>;
        fn instance_handle() -> &'static str {
            "ops"
        }
        fn table_name() -> &'static str {
            "song"
        }
    }

    pub struct Context {
        client_id: Option<i64>,
        instances: DbrInstances,
        pool: mysql_async::Pool,
    }

    impl Context {
        pub fn client_tag(&self) -> Option<String> {
            self.client_id.map(|client_id| format!("c{}", client_id))
        }
    }

    let mut instances = DbrInstances::new();

    let opts = mysql::Opts::from_url("mysql://devuser:password@localhost:3306/dbr")?;
    let mut metadata_conn = mysql::Conn::new(opts)?;
    let all_instances = DbrInstanceInfo::fetch_all(&mut metadata_conn)?;
    for info in all_instances {
        instances.insert(DbrInstance::new(info));
    }

    let context = Context {
        client_id: Some(1),
        instances: instances,
        pool: pool,
    };

    let songs = {
        async fn song_fetch_internal(context: &Context) -> Result<Vec<Active<Song>>, DbrError> {
            let mut connection = context.pool.get_conn().await?;
            let instance = context
                .instances
                .lookup_by_handle(Song::instance_handle().to_owned(), context.client_tag())
                .ok_or(DbrError::MissingStore(Song::instance_handle().to_owned()))?;

            const MYSQL_QUERY: &'static str = r#"SELECT song.id, song.name, song.album_id FROM song JOIN album ON (song.album_id = album.id) JOIN artist ON (album.artist_id = artist.id) WHERE artist.genre = "Math rock""#;
            const SQLITE_QUERY: &'static str = r#"SELECT id, name, album_id FROM song JOIN album ON (song.album_id = album.id) JOIN artist ON (album.artist_id = artist.id) WHERE artist.genre = "Rock""#;

            let result_set: Vec<Song>;
            result_set = MYSQL_QUERY
                .with(())
                .map(&mut connection, |(id, name, album_id)| Song {
                    id,
                    name,
                    album_id,
                })
                .await?;

            let mut active_records: Vec<Active<Song>> = Vec::new();
            for record in result_set {
                let id = record.id;
                let record_ref = instance.cache.set_record(id, record)?;
                active_records.push(Active::<Song>::from_arc(id, record_ref));
            }

            Ok(active_records)
        }

        song_fetch_internal(&context).await
    }?;

    dbg!(songs);

    // Dropped connection will go to the pool
    drop(conn);

    // The Pool must be disconnected explicitly because
    // it's an asynchronous operation.
    context.pool.disconnect().await?;

    // the async fn returns Result, so
    Ok(())
}
