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
use rust_dbr::{instance::Pool, prelude::*};
use sqlx::{FromRow, mysql::MySqlArguments, Arguments};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = "mysql://devuser:password@localhost:3306/account_test";
    let dbr_url = "mysql://devuser:password@localhost:3306/dbr";

    use sqlx::mysql::MySqlPool;

    let pool = MySqlPool::connect(dbr_url).await?;

    let mut instances = DbrInstances::new();

    let all_instances = DbrInstanceInfo::fetch_all(&pool).await?;
    for info in all_instances {
        instances.insert(DbrInstance::new(info));
    }

    let context = Context {
        client_id: Some(1),
        instances: instances,
    };
    // let songs: Vec<Active<Song>> = fetch!(&context, Song where album.artist.genre = 'Something')?;
    // expands out to ->
    let mut songs = {
        async fn song_fetch_internal(context: &Context) -> Result<Vec<Active<Song>>, DbrError> {
            /*
            let instance = context
                .instances
                .lookup_by_handle(Song::schema().to_owned(), context.client_tag())
                .ok_or(DbrError::MissingStore(Song::schema().to_owned()))?;

            const MYSQL_QUERY: &'static str = r#"SELECT song.id, song.name, song.album_id, song.likes FROM song JOIN album ON (song.album_id = album.id) JOIN artist ON (album.artist_id = artist.id) WHERE artist.genre = "Math rock""#;
            const SQLITE_QUERY: &'static str = r#"SELECT id, name, album_id FROM song JOIN album ON (song.album_id = album.id) JOIN artist ON (album.artist_id = artist.id) WHERE artist.genre = "Rock""#;

            let result_set: Vec<Song> = match &instance.pool {
                Pool::MySql(pool) => sqlx::query_as(MYSQL_QUERY).fetch_all(pool).await?,
                Pool::Sqlite(pool) => sqlx::query_as(SQLITE_QUERY).fetch_all(pool).await?,
                Pool::Disconnected => {
                    return Err(DbrError::PoolDisconnected);
                }
            };

            for record in result_set {
                let id = record.id;
                let record_ref = instance.cache.set_record(id, record)?;
                active_records.push(Active::<Song>::from_arc(id, record_ref));
            }
            */
            let mut active_records: Vec<Active<Song>> = Vec::new();
            Ok(active_records)
        }

        song_fetch_internal(&context).await
    }?;

    for song in &mut songs {
        let id = song.id();
        let name = song.name()?;
        let album_id = song.album_id()?;
        let likes = song.likes()?;
        dbg!(&id, &name, &album_id);
        song.set_name(&context, song.name()?).await?;
        song.set_album_id(&context, song.album_id()?).await?;
        dbg!(song.likes()?);
        song.set_likes(&context, song.likes()? + 1).await?;
    }
    //let rootContext = dbr!();
    //let fooCo = rootContext.subContext("foo");
    //let trx = myContext.start_transaction();

    //let artists: Vec<Active<Song>> = fetch!(&trx, Artist where album.song.title like "%Baby%")?;
    /*
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
           song.set_name(song.name()?.clone() + " asdf");
           song.set_album_id(song.album_id());
       }
    */
    /*
        let song = Active::<Song>::create(&context, PartialSong {
            id: 1, // how should we express partials here?
            name: "Something".to_owned(),
            album_id: 1,
        });
    */

    Ok(())
}

#[derive(FromRow, Debug, Clone)]
pub struct Song {
    id: i64,
    name: String,
    album_id: i64,
    likes: i64,
}

impl DbrTable for Song {
    type ActiveModel = Active<Song>;
    type PartialModel = PartialSong;
    fn schema() -> &'static str {
        "ops"
    }
    fn table_name() -> &'static str {
        "song"
    }
}

/// After this mark - all of this will be macro expansions
#[derive(Debug, Clone)]
pub struct PartialSong {
    id: Option<i64>,
    name: Option<String>,
    album_id: Option<i64>,
    likes: Option<i64>,
}

impl Default for PartialSong {
    fn default() -> Self {
        Self {
            id: None,
            name: None,
            album_id: None,
            likes: None,
        }
    }
}

impl PartialModel<Song> for PartialSong {
    fn apply<B: std::ops::Deref<Target = Song> + std::ops::DerefMut>(
        self,
        mut record: &mut B,
    ) -> Result<(), DbrError> {
        let PartialSong {
            id,
            name,
            album_id,
            likes,
        } = self;

        if let Some(id) = id {
            record.id = id;
        }

        if let Some(name) = name {
            record.name = name;
        }

        if let Some(album_id) = album_id {
            record.album_id = album_id;
        }

        if let Some(likes) = likes {
            record.likes = likes;
        }

        Ok(())
    }
    fn id(&self) -> Option<i64> {
        self.id
    }
}

#[async_trait]
pub trait SongFields {
    fn name(&self) -> Result<String, DbrError>;
    fn album_id(&self) -> Result<i64, DbrError>;
    fn likes(&self) -> Result<i64, DbrError>;

    async fn set(&mut self, context: &Context, song: PartialSong) -> Result<(), DbrError>;
    async fn set_name<T: Into<String> + Send>(
        &mut self,
        context: &Context,
        name: T,
    ) -> Result<(), DbrError>;
    async fn set_album_id<T: Into<i64> + Send>(
        &mut self,
        context: &Context,
        album_id: T,
    ) -> Result<(), DbrError>;
    async fn set_likes<T: Into<i64> + Send>(
        &mut self,
        context: &Context,
        likes: T,
    ) -> Result<(), DbrError>;
}

#[async_trait]
impl SongFields for Active<Song> {
    fn name(&self) -> Result<String, DbrError> {
        let snapshot = self.snapshot()?;
        Ok(snapshot.name)
    }
    fn album_id(&self) -> Result<i64, DbrError> {
        let snapshot = self.snapshot()?;
        Ok(snapshot.album_id)
    }
    fn likes(&self) -> Result<i64, DbrError> {
        let snapshot = self.snapshot()?;
        Ok(snapshot.likes)
    }
    async fn set(&mut self, context: &Context, song: PartialSong) -> Result<(), DbrError> {
        let instance = context.instance_by_handle(Song::schema().to_owned())?;
        let song_partial = song.clone();
        match &instance.pool {
            Pool::MySql(pool) => {
                let mut fields = Vec::new();
                let mut arguments = MySqlArguments::default();

                if let Some(name) = song.name {
                    fields.push("name = ?");
                    arguments.add(name);
                }

                if let Some(album_id) = song.album_id {
                    fields.push("album_id = ?");
                    arguments.add(album_id);
                }

                if let Some(likes) = song.likes {
                    fields.push("likes = ?");
                    arguments.add(likes);
                }

                if fields.len() == 0 {
                    return Ok(())
                }

                arguments.add(self.id());
                let query_str = format!("UPDATE {} SET {} WHERE id = ?", Song::table_name(), fields.join(" "));
                let query = sqlx::query_with(&query_str, arguments);
                query.execute(pool).await?;
            }
            Pool::Sqlite(pool) => {
                let query = format!("UPDATE {} WHERE id = ?", Song::table_name());
                sqlx::query("UPDATE {}").execute(pool).await?;
            }
            Pool::Disconnected => {
                return Err(DbrError::PoolDisconnected);
            }
        }

        self.apply_partial(song_partial)?;
        Ok(())
    }
    async fn set_name<T: Into<String> + Send>(
        &mut self,
        context: &Context,
        name: T,
    ) -> Result<(), DbrError> {
        self.set(
            context,
            PartialSong {
                name: Some(name.into()),
                ..Default::default()
            },
        )
        .await
    }
    async fn set_album_id<T: Into<i64> + Send>(
        &mut self,
        context: &Context,
        album_id: T,
    ) -> Result<(), DbrError> {
        self.set(
            context,
            PartialSong {
                album_id: Some(album_id.into()),
                ..Default::default()
            },
        )
        .await
    }
    async fn set_likes<T: Into<i64> + Send>(
        &mut self,
        context: &Context,
        likes: T,
    ) -> Result<(), DbrError> {
        self.set(
            context,
            PartialSong {
                likes: Some(likes.into()),
                ..Default::default()
            },
        )
        .await
    }
}
