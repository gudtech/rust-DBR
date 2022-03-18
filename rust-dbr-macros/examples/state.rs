
use rust_dbr::{
    metadata::Metadata,
    prelude::*,
};
use rust_dbr_macros::{fetch, DbrTable};

/*
MySQL [constants]> select * from states limit 10;
+----+------------+-----------------------+------+---------+-----------+
| id | country_id | name                  | abbr | sortval | iso3166_2 |
+----+------------+-----------------------+------+---------+-----------+
|  1 |          1 | Armed Forces Europe   | AE   |       1 | NULL      |
|  2 |          1 | Armed Forces Americas | AA   |       2 | NULL      |
|  3 |          1 | Armed Forces Pacific  | AP   |       3 | NULL      |
|  4 |          1 | Alabama               | AL   |       4 | NULL      |
|  5 |          1 | Alaska                | AK   |       5 | NULL      |
|  6 |          1 | American Samoa        | AS   |       6 | NULL      |
|  7 |          1 | Arizona               | AZ   |       7 | NULL      |
|  8 |          1 | Arkansas              | AR   |       8 | NULL      |
|  9 |          1 | California            | CA   |       9 | NULL      |
| 10 |          1 | Colorado              | CO   |      10 | NULL      |
+----+------------+-----------------------+------+---------+-----------+
10 rows in set (0.00 sec)

MySQL [constants]> show create table states;
+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table  | Create Table                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| states | CREATE TABLE `states` (
  `id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT,
  `country_id` mediumint(8) unsigned DEFAULT NULL,
  `name` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `abbr` char(8) COLLATE utf8mb4_unicode_ci NOT NULL,
  `sortval` int(10) unsigned NOT NULL,
  `iso3166_2` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `country` (`country_id`),
  CONSTRAINT `fk_states_country` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=731 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci |
*/

#[derive(DbrTable, sqlx::FromRow, Debug, Clone)]
#[table = "constants.states"]
pub struct State {
    id: u32,
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
            and country_id = country_id).await?
    } else {
        fetch!(&context, State where name like format!("%{}%", query)
            and countries.code2 = "US").await?
    };

    for state in states {
        let country = state.country_id()?;
        let name = state.name()?;
        dbg!(state.id(), country, name);
    }

    Ok(())
}

