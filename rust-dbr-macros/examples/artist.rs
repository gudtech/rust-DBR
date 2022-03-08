use rust_dbr::prelude::*;
use rust_dbr_macros::{fetch, DbrTable};
use std::{collections::HashMap, sync::Arc};
//use dbr_sample_dataset::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    artist_example()
}

async fn artist_example() -> Result<(), DbrError> {
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

    //context.shutdown().await?;
    Ok(())
}

#[derive(DbrTable, Debug, Clone)]
#[table = "artist"]
pub struct Artist {
    id: i64,
    name: String,
}

#[derive(DbrTable, Debug, Clone)]
#[table = "album"]
pub struct Album {
    id: i64,
    artist_id: i64,
    name: String,
    date_released: u64,
}

#[derive(DbrTable, Debug, Clone)]
#[table = "song"]
pub struct Song {
    id: i64,
    name: String,
    album_id: i64,
}
