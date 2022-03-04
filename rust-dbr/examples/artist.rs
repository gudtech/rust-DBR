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
    Active, DbrError, DbrInstance, DbrInstances, DbrRecordStore, DbrTable,
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
    let payments = vec![
        Payment {
            customer_id: 1,
            amount: 2,
            account_name: None,
        },
        Payment {
            customer_id: 3,
            amount: 4,
            account_name: Some("foo".into()),
        },
        Payment {
            customer_id: 5,
            amount: 6,
            account_name: None,
        },
        Payment {
            customer_id: 7,
            amount: 8,
            account_name: None,
        },
        Payment {
            customer_id: 9,
            amount: 10,
            account_name: Some("bar".into()),
        },
    ];

    let database_url = "";

    let pool = mysql_async::Pool::new(database_url);
    let mut conn = pool.get_conn().await?;

    // Create a temporary table
    r"CREATE TEMPORARY TABLE payment (
        customer_id int not null,
        amount int not null,
        account_name text
    )"
    .ignore(&mut conn)
    .await?;

    // Save payments
    r"INSERT INTO payment (customer_id, amount, account_name)
      VALUES (:customer_id, :amount, :account_name)"
        .with(payments.iter().map(|payment| {
            params! {
                "customer_id" => payment.customer_id,
                "amount" => payment.amount,
                "account_name" => payment.account_name.as_ref(),
            }
        }))
        .batch(&mut conn)
        .await?;

    //fetch!(Song where album.artist.genre = "Rock");
    // SELECT id, name FROM song JOIN album ON (song.album_id = album.id) JOIN artist ON (album.artist_id = artist.id) WHERE artist.genre = "Rock";

    // Load payments from the database. Type inference will work here.
    let loaded_payments = "SELECT customer_id, amount, account_name FROM payment"
        .with(())
        .map(&mut conn, |(customer_id, amount, account_name)| Payment {
            customer_id,
            amount,
            account_name,
        });

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

    let instances = DbrInstances::new();

    let opts = mysql::Opts::from_url("mysql://devuser:password@localhost:3306/dbr")?;
    let mut metadata_conn = mysql::Conn::new(opts)?;
    let all_instances = DbrInstance::fetch_all(&mut metadata_conn)?;
    for instance in all_instances {
        instances.insert(instance);
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

            const MYSQL_QUERY: &'static str = r#"SELECT id, name, album_id FROM song JOIN album ON (song.album_id = album.id) JOIN artist ON (album.artist_id = artist.id) WHERE artist.genre = "Rock""#;
            const SQLITE_QUERY: &'static str = r#"SELECT id, name, album_id FROM song JOIN album ON (song.album_id = album.id) JOIN artist ON (album.artist_id = artist.id) WHERE artist.genre = "Rock""#;

            let result_set = match instance.info.module() {
                StoreModule::Mysql => {
                    MYSQL_QUERY
                        .with(())
                        .map(&mut connection, |(id, name, album_id)| Song {
                            id,
                            name,
                            album_id,
                        })
                        .await?
                }
                StoreModule::SQLite => Vec::new(),
            };

            let mut active_records: Vec<Active<Song>> = Vec::new();
            for song in result_set {
                let id = song.id;
                let record_ref = store.set_record(id, song)?;
                active_records.push(Active::<Song>::from_arc(id, record_ref));
            }

            Ok(active_records)
        }

        song_fetch_internal(&context).await
    }?;

    let loaded_payments = loaded_payments.await?;

    // Dropped connection will go to the pool
    drop(conn);

    // The Pool must be disconnected explicitly because
    // it's an asynchronous operation.
    context.pool.disconnect().await?;

    assert_eq!(loaded_payments, payments);

    // the async fn returns Result, so
    Ok(())
}
