
use mysql::prelude::*;

#[derive(Debug, Clone)]
pub struct Instance {
    pub id: i64,
    pub schema_id: i64,
    pub handle: String,
    pub class: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub host: String,
    pub module: String,
    pub tag: Option<String>,
}

impl Instance {
    pub fn fetch_all(conn: &mut mysql::Conn) -> Result<Vec<Instance>, mysql::Error> {
        conn.query_map(
            "SELECT instance_id, schema_id, handle, class, dbname, username, password, host, module, tag FROM dbr.dbr_instances",
            |(id, schema_id, handle, class, database, username, password, host, module, tag)| {
                Self {id, schema_id, handle, class, database, username, password, host, module, tag}
            }
        )
    }

    pub fn connection(&self) -> Result<mysql::Conn, mysql::Error> {
        let mut host = self.host.clone();
        host = "localhost".to_owned(); // for testing purposes
        let opts = mysql::Opts::from_url(&format!("mysql://{user}:{pass}@{host}:3306/{db}", user = self.username, pass = self.password, host = host, db = self.database))?;

        mysql::Conn::new(opts)
    }
}
