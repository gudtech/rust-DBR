use rust_dbr::{
    context::{RelationJoin, RelationPath},
    metadata::Metadata,
    prelude::*,
};
use rust_dbr_macros::{fetch, DbrTable};
//use dbr_sample_dataset::*;

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
    //#[relation(Artist)]
    artist_id: i64,
    name: String,
    date_released: i64,
}

#[derive(DbrTable, sqlx::FromRow, Debug, Clone)]
#[table = "ops.song"]
pub struct Song {
    id: i64,
    //#[relation(Album)]
    album_id: i64,
    name: String,
    likes: i64,
}

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

    //let database_url = "mysql://devuser:password@localhost:3306/account_test";
    let dbr_url = "mysql://devuser:password@localhost:3306/dbr";

    let pool = sqlx::mysql::MySqlPool::connect(dbr_url).await?;
    let connection = pool.acquire().await?;
    let metadata = Metadata::fetch(connection).await?;

    let mut instances = DbrInstances::new();

    let all_instances = DbrInstanceInfo::fetch_all(&pool).await?;
    for mut info in all_instances {
        info.set_host("localhost:3306".to_owned());
        let instance = DbrInstance::new(info).await?;
        instances.insert(instance);
    }

    let context = Context {
        client_id: Some(1),
        instances: instances,
        metadata: metadata,
    };

    pub struct MyStruct { somethin: i64, }
    let something = MyStruct {
        somethin: 1,
    };
    let name = "%t%";
    let x = 4;
    let mut songs: Vec<Active<Song>> = fetch!(
        &context,
        Song where
            name like something
            and album.artist.genre like "math%"
            and (album.artist.genre like "%rock%" or album.id = 4i64)
        order by id
        limit 1i64
    ).await?;

    /*
    for song in &mut songs {
        let id = song.id();
        let name = song.name()?;
        let album_id = song.album_id()?;
        dbg!(&id, &name, &album_id);
        song.set_name(&context, song.name()?).await?;
        song.set_album_id(&context, song.album_id()?).await?;
        dbg!(&song.likes()?);
        song.set_likes(&context, song.likes()? + 1).await?;
        dbg!(&song.likes()?);
    }
 */
    Ok(())
}
