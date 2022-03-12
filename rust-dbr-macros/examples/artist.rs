use rust_dbr::{metadata::Metadata, prelude::*};
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

    dbg!();
    //let mut songs: Vec<Active<Song>> = fetch!(&context, Song where album.artist.genre = "Something" and album.name = "asdf" order by id, name limit 1000)?;
    // expands out to ->
    let mut songs = {
        async fn __fetch_internal(context: &Context) -> Result<Vec<Active<Song>>, DbrError> {
            let instance = context.instance_by_handle(Song::schema().to_owned())?;
            let schema = context
                .metadata
                .lookup_schema(SchemaIdentifier::Name(Song::schema().to_owned()))?;

            let mut fields = Vec::new();
            fields.push("id");
            fields.push("name");
            fields.push("album_id");
            fields.push("likes");

            let mut join = Vec::new();

            {
                let relation = context.metadata.find_relation(
                    SchemaIdentifier::Name(Song::schema().to_owned()),
                    TableIdentifier::Name(Song::table_name().to_owned()),
                    TableIdentifier::Name("album".to_owned()),
                )?;

                let to_table = context.metadata.lookup_table(relation.to_table_id)?;
                let to_field = context.metadata.lookup_field(relation.to_field_id)?;

                let from_table = context.metadata.lookup_table(relation.from_table_id)?;
                let from_field = context.metadata.lookup_field(relation.from_field_id)?;

                let to_schema = context
                    .metadata
                    .lookup_schema(SchemaIdentifier::Id(to_table.schema_id))?;

                let album_instance = context.instance_by_handle(to_schema.name.to_owned())?;

                if instance.info.colocated(&album_instance.info) {
                    join.push(format!(
                        "JOIN {}.{} ON ({}.{} = {}.{}",
                        album_instance.info.database_name(),
                        to_table.name,
                        to_table.name,
                        to_field.name,
                        from_table.name,
                        from_field.name,
                    ));
                } else {
                    // we need to do a subquery...
                }
            };

            let result_set: Vec<Song> = match &instance.pool {
                Pool::MySql(pool) => {
                    let fields_select: Vec<_> = fields
                        .iter()
                        .map(|field| format!("{}.{}", Song::table_name(), field))
                        .collect();
                    let base_name =
                        format!("{}.{}", instance.info.database_name(), Song::table_name());
                    let query = format!("SELECT {} FROM {}", fields_select.join(", "), base_name);
                    dbg!(&query);
                    sqlx::query_as(&query).fetch_all(pool).await?
                }
                _ => Vec::new(),
            };

            let mut active_records: Vec<Active<Song>> = Vec::new();
            for record in result_set {
                let id = record.id;
                let record_ref = instance.cache.set_record(id, record)?;
                active_records.push(Active::<Song>::from_arc(id, record_ref));
            }

            Ok(active_records)
        }

        __fetch_internal(&context).await
    }?;

    dbg!();
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

/*
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
    date_released: u64,
}
*/

#[derive(DbrTable, sqlx::FromRow, Debug, Clone)]
#[table = "ops.song"]
pub struct Song {
    id: i64,
    //#[relation(Album)]
    album_id: i64,
    name: String,
    likes: i64,
}
