use rust_dbr::query::queryable::DbrObject;


#[derive(Dbr, Debug)]
pub struct Artist {
    id: i64,
    name: Option<String>,
}

pub struct Album {
    id: i64,
    artist_id: i64,
    name: Option<String>,
    date_released: Option<u64>,
}

pub struct Context {
    client: Client,
    conn: mysql::Conn,
}

pub struct Request {
    request_id: i64,
}

pub struct Response { }

pub fn get_albums(context: Context, params: RequestParams) -> Result<Response, Error> {
    let artists = Artist::fetch_all();

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