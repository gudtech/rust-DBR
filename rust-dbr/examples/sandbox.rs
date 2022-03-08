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
use rust_dbr::query::queryable::*;

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
    let mut songs = {
        async fn song_fetch_internal(context: &Context) -> Result<Vec<Active<Song>>, DbrError> {
            let mut connection = context.pool.get_conn().await?;
            let instance = context
                .instances
                .lookup_by_handle(Song::instance_handle().to_owned(), context.client_tag())
                .ok_or(DbrError::MissingStore(Song::instance_handle().to_owned()))?;

            const MYSQL_QUERY: &'static str = r#"SELECT song.id, song.name, song.album_id, song.likes FROM song JOIN album ON (song.album_id = album.id) JOIN artist ON (album.artist_id = artist.id) WHERE artist.genre = "Math rock""#;
            const SQLITE_QUERY: &'static str = r#"SELECT id, name, album_id FROM song JOIN album ON (song.album_id = album.id) JOIN artist ON (album.artist_id = artist.id) WHERE artist.genre = "Rock""#;

            let result_set: Vec<Song>;
            result_set = MYSQL_QUERY
                .with(())
                .map(&mut connection, |(id, name, album_id, likes)| Song {
                    id,
                    name,
                    album_id,
                    likes,
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

    for song in &mut songs {
        let id = song.id();
        let name = song.name()?;
        let album_id = song.album_id()?;
        let likes = song.likes()?;
        dbg!(&id, &name, &album_id);
        song.set_name(&context, song.name).await?;
        song.set_album_id(&context, album_id).await?;
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

    // Dropped connection will go to the pool
    drop(conn);

    // The Pool must be disconnected explicitly because
    // it's an asynchronous operation.
    context.pool.disconnect().await?;

    Ok(())
}

#[derive(Debug, Clone)]
pub struct Song {
    id: i64,
    name: String,
    album_id: i64,
    likes: i64,
}

impl DbrTable for Song {
    type ActiveModel = Active<Song>;
    type PartialModel = PartialSong;
    fn instance_handle() -> &'static str {
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
        let mut connection = context.pool.get_conn().await?;
        let mut params: HashMap<String, mysql_async::Value> = HashMap::new();
        let mut set_fields = Vec::new();
        params.insert("id".to_owned(), self.id().into());

        let song_partial = song.clone();

        if let Some(id) = song.id {
            params.insert("set_id".to_owned(), id.into());
            set_fields.push("id = :set_id");
        }

        if let Some(name) = song.name {
            params.insert("set_name".to_owned(), name.into());
            set_fields.push("name = :set_name");
        }

        if let Some(album_id) = song.album_id {
            params.insert("set_album_id".to_owned(), album_id.into());
            set_fields.push("album_id = :set_album_id");
        }

        if let Some(likes) = song.likes {
            params.insert("set_likes".to_owned(), likes.into());
            set_fields.push("likes = :set_likes");
        }

        if params.len() == 0 {
            return Ok(());
        }

        let MYSQL_QUERY = format!(
            r#"UPDATE song SET {} WHERE id = :id"#,
            set_fields.join(", ")
        );

        connection
            .exec::<mysql_async::Row, _, _>(MYSQL_QUERY, mysql::Params::Named(params))
            .await?;
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
