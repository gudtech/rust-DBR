use rust_dbr::query::queryable::{Active, DbrError};
use std::{collections::HashMap, sync::Arc};
//use dbr_sample_dataset::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    artist_example()
}

async fn artist_example() -> Result<(), DbrError> {
    //let root_context = DBRSampleDataSet::init_sample_dataset(); // Typically you would DBR::load_metadata("file or DB url")
    let context = Context { client_id: None };
    //let foo_co = rootContext.subContext("foo");
    let trx = root_context.start_transaction()?;

    let artists: Vec<Active<Song>> = fetch!(&trx, Artist where album.song.title like "%Baby%")?;
    for artist in artists {
        println!("Artist: {}", artist.name()?);
        for album in artist.albums().await {
            println!("\tAlbum: {}", album.name()?);
            for song in album.songs().await {
                println!("\t\tSong: {}", song.name()?);

                let new_name = song.name()?.replace("baby", "child");
                song.set_name(trx, new_name)?;
            }
        }
    }
    trx.commit()?;

    rootContext.shutdown().await?;
    Ok(())
}

#[derive(DbrTable, Debug, Clone)]
#[dbr(table_name = "artist")]
pub struct Artist {
    id: i64,
    name: String,
}

#[derive(DbrTable, Debug, Clone)]
#[dbr(table_name = "album")]
pub struct Album {
    id: i64,
    artist_id: i64,
    name: String,
    date_released: u64,
}

#[derive(DbrTable, Debug, Clone)]
#[dbr(table_name = "song")]
pub struct Song {
    id: i64,
    name: String,
    album_id: i64,
}
