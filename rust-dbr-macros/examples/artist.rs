use rust_dbr::prelude::*;
use rust_dbr_macros::{fetch, DbrTable};
//use dbr_sample_dataset::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //let root_context = DBRSampleDataSet::init_sample_dataset(); // Typically you would DBR::load_metadata("file or DB url")
    //let context = Context { client_id: None };

    /*
    let artists: Vec<Active<Song>> = fetch!(&context, Artist where album.song.title like "%Baby%")?;
    for artist in artists {
        println!("Artist: {}", artist.name()?);
        for album in artist.albums().await {
            println!("\tAlbum: {}", album.name()?);
            for song in album.songs().await {
                println!("\t\tSong: {}", song.name()?);

                let new_name = song.name()?.replace("baby", "child");
                song.set_name(&context, new_name)?;
            }
        }
    } */

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

            use mysql_async::prelude::*;

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
        song.set_name(&context, song.name()?).await?;
        song.set_album_id(&context, song.album_id()?).await?;
        dbg!(song.likes()?);
        song.set_likes(&context, song.likes()? + 1).await?;
        dbg!(song.likes()?);
    }

    //context.shutdown().await?;
    Ok(())
}

#[derive(DbrTable, Debug, Clone)]
#[table = "ops.artist"]
pub struct Artist {
    id: i64,
    name: String,
}

#[derive(DbrTable, Debug, Clone)]
#[table = "ops.album"]
pub struct Album {
    id: i64,
    artist_id: i64,
    name: String,
    date_released: u64,
}

#[derive(DbrTable, Debug, Clone)]
#[table = "ops.song"]
pub struct Song {
    id: i64,
    name: String,
    album_id: i64,
    likes: i64,
}