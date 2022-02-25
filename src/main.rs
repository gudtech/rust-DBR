
use rust_dbr::config::instance::Instance;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use mysql::*;
    use mysql::prelude::*;

    let opts = Opts::from_url("mysql://devuser:password@localhost:3306/")?;
    //let sqlite = sqlite::open(":memory:");

    let mut conn = Conn::new(opts)?;
    let val: Vec<String> = conn.query("SHOW TABLES FROM dbr")?;

    let mut instances = Instance::fetch_all(&mut conn)?;
    dbg!(&instances);

    let instance = instances.iter().filter(|instance| instance.tag == Some("c1".to_owned())).next().unwrap();
    dbg!(&instance);
    let mut c1_conn = instance.connection()?;
    let values: Vec<mysql::Row> = c1_conn.query("SELECT * FROM customer_order")?;
    dbg!(values);

    Ok(())
}
