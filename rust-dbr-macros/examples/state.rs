use rust_dbr::{metadata::Metadata, prelude::*};
use rust_dbr_macros::{fetch, DbrTable};

#[derive(DbrTable, sqlx::FromRow, Debug, Clone)]
#[table = "constants.states"]
pub struct State {
    id: u32,
    // not entirely sure why this is null
    country_id: Option<u32>,
    name: String,
    abbr: String,
    sortval: u32,
    iso3166_2: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let query = "";
    let country_id: Option<i32> = None;
    let states = if let Some(country_id) = country_id {
        fetch!(&context, State where name like format!("%{}%", query)
            and country_id = country_id)
        .await?
    } else {
        fetch!(&context, State where name like format!("%{}%", query)
            and countries.code2 = "US")
        .await?
    };

    for state in states {
        let country = state.country_id()?;
        let name = state.name()?;
        dbg!(state.id(), country, name);
    }

    Ok(())
}
