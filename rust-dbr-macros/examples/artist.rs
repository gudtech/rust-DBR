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

    let mut songs: Vec<Active<Song>> = fetch!(
        &context,
        Song where
            name = "The Detail"
            and album.artist.genre like "math%"
            and (album.artist.genre like "%rock%" or album.id = 4i64)
        order by id
        limit 10
    )?;

    /*
    let mut songs: Vec<Active<Song>> = {
        async fn __fetch_internal(
            context: &::rust_dbr::Context,
        ) -> Result<Vec<::rust_dbr::Active<Song>>, ::rust_dbr::DbrError> {
            use ::sqlx::Arguments;
            let instance = context.instance_by_handle(Song::schema().to_owned())?;
            let schema = context
                .metadata
                .lookup_schema(::rust_dbr::SchemaIdentifier::Name(
                    Song::schema().to_owned(),
                ))?;
            let base_table_id = schema.lookup_table_by_name(Song::table_name().to_owned())?;
            let base_table = context.metadata.lookup_table(*base_table_id)?;
            let mut select = ::rust_dbr::Select::new(*base_table_id);
            select.filters = Some(::rust_dbr::FilterTree::And {
                children: <[_]>::into_vec(Box::new([
                    ::rust_dbr::FilterTree::Predicate(::rust_dbr::FilterPredicate {
                        path: ::rust_dbr::RelationPath {
                            base: *base_table_id,
                            relations: ::std::vec::Vec::new().into(),
                            field: "name".to_owned(),
                        },
                        op: ::rust_dbr::FilterOp::Eq,
                        value: {
                            use ::sqlx::Arguments;
                            let mut args = ::sqlx::any::AnyArguments::default();
                            args.add("The Detail");
                            args
                        },
                    }),
                    ::rust_dbr::FilterTree::And {
                        children: <[_]>::into_vec(Box::new([
                            ::rust_dbr::FilterTree::Predicate(::rust_dbr::FilterPredicate {
                                path: ::rust_dbr::RelationPath {
                                    base: *base_table_id,
                                    relations: <[_]>::into_vec(Box::new([
                                        "album".to_owned(),
                                        "artist".to_owned(),
                                    ]))
                                    .into(),
                                    field: "genre".to_owned(),
                                },
                                op: ::rust_dbr::FilterOp::Like,
                                value: {
                                    use ::sqlx::Arguments;
                                    let mut args = ::sqlx::any::AnyArguments::default();
                                    args.add("math%");
                                    args
                                },
                            }),
                            ::rust_dbr::FilterTree::Or {
                                left: Box::new(::rust_dbr::FilterTree::Predicate(
                                    ::rust_dbr::FilterPredicate {
                                        path: ::rust_dbr::RelationPath {
                                            base: *base_table_id,
                                            relations: <[_]>::into_vec(Box::new([
                                                "album".to_owned(),
                                                "artist".to_owned(),
                                            ]))
                                            .into(),
                                            field: "genre".to_owned(),
                                        },
                                        op: ::rust_dbr::FilterOp::Like,
                                        value: {
                                            use ::sqlx::Arguments;
                                            let mut args = ::sqlx::any::AnyArguments::default();
                                            args.add("%rock%");
                                            args
                                        },
                                    },
                                )),
                                right: Box::new(::rust_dbr::FilterTree::Predicate(
                                    ::rust_dbr::FilterPredicate {
                                        path: ::rust_dbr::RelationPath {
                                            base: *base_table_id,
                                            relations: <[_]>::into_vec(
                                                Box::new(["album".to_owned()]),
                                            )
                                            .into(),
                                            field: "id".to_owned(),
                                        },
                                        op: ::rust_dbr::FilterOp::Eq,
                                        value: {
                                            use ::sqlx::Arguments;
                                            let mut args = ::sqlx::any::AnyArguments::default();
                                            args.add(4i64);
                                            args
                                        },
                                    },
                                )),
                            },
                        ])),
                    },
                ])),
            });
            select.fields = base_table.fields.values().cloned().collect();
            let resolved_select = select.resolve(context)?;
            let (sql, args) = match resolved_select.as_sql() {
                Some((sql, args)) => {
                    (sql, args)
                }
                _ => (String::new(), sqlx::any::AnyArguments::default()),
            };

            dbg!(&sql);
            let result_set: Vec<Song> = sqlx::query_as_with(&sql, args)
                .fetch_all(&instance.pool)
                .await?
                .clone();
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
    */

    /*
       //let mut songs: Vec<Active<Song>> = fetch!(&context, Song where album.artist.genre like "math%".to_string())?;
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
