#![feature(generic_associated_types)]
//use rust_dbr::query::queryable::DbrObject;

// #[dbr(table_name = "artist")]
#[derive(Debug)]
pub struct Artist {
    id: i64,
    name: String,
}

// #[dbr(table_name = "album")]
#[derive(Debug)]
pub struct Album {
    id: i64,
    artist_id: i64,
    name: String,
    date_released: u64,
}

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use futures::future::BoxFuture;
use mysql_async::prelude::*;
use rust_dbr::query::queryable::{
    Active, DbrError, DbrInstance, DbrInstances, DbrRecordCache, DbrTable, DbrInstanceInfo, ActiveModel,
};


#[derive(Debug, Clone)]
pub struct Song {
    id: i64,
    name: String,
    album_id: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = "mysql://devuser:password@localhost:3306/account_test";

    let pool = mysql_async::Pool::new(database_url);
    let mut conn = pool.get_conn().await?;

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

    // let songs: Vec<Active<Song>> = fetch!(&context, Song where album.artist.genre = 'Something')?;
    // expands out to ->
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

    
    let rootContext = dbr!();
    let fooCo = rootContext.subContext('foo');
    let trx = myContext.start_transaction();

    let artists: Vec<Active<Song>> = fetch!(&trx, Artist where album.song.title like '%Baby%')?;
for artist in artists {
    println!("Artist: {artist.name}");
    for album in artist.albums().await {
        println!("\tAlbum: {album.name}");
        for song in album.songs().await {
            println!("\t\tSong: {song.name}");

            let new_name = song.name().replace("baby", "child");
            song.set_name(trx, new_name);
        }
    }
    trx.commit()
}

    for song in songs {
        song.set_name(song.name.clone() + " asdf"); 
        song.set_album_id(song.album_id); 
    }

    /*
        let song = Active::<Song>::create(&context, PartialSong {
            id: 1, // how should we express partials here?
            name: "Something".to_owned(),
            album_id: 1,
        });
    */


    dbg!(songs);

    // Dropped connection will go to the pool
    drop(conn);

    // The Pool must be disconnected explicitly because
    // it's an asynchronous operation.
    context.pool.disconnect().await?;

    Ok(())
}

/// After this mark - all of this will be macro expansions
pub struct PartialSong {
    id: Option<i64>,
    name: Option<String>,
    album_id: Option<i64>,
}

impl DbrTable for Song {
    type ActiveModel = Active<Song>;
    type Partial = PartialSong;
    fn instance_handle() -> &'static str {
        "ops"
    }
    fn table_name() -> &'static str {
        "song"
    }
}

#[async_trait]
pub trait SongFields {
    fn name(&self) -> Result<&String, DbrError>;
    fn album_id(&self) -> Result<&i64, DbrError>;

    async fn set_name(&mut self, name: String) -> Result<(), DbrError>;
}

#[async_trait]
impl SongFields for Active<Song> {
    fn name(&self) -> Result<&String, DbrError> {
        let snapshot = self.snapshot()?;
        Ok(&snapshot.name)
    }
    fn album_id(&self) -> Result<&i64, DbrError> {
        let snapshot = self.snapshot()?;
        Ok(&snapshot.album_id)
    }
    async fn set_name(&mut self, context: &Context, name: String) -> Result<(), DbrError> {
        let mut connection = context.pool.get_conn().await?;
        const MYSQL_QUERY: &'static str = r#"UPDATE song SET name = :name WHERE id = :id"#;

        MYSQL_QUERY.

        Ok(())
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
