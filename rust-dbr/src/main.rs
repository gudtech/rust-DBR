use rust_dbr::query::queryable::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use mysql::prelude::*;
    use mysql::*;

    let opts = Opts::from_url("mysql://devuser:password@localhost:3306/dbr")?;
    let mut metadata_conn = Conn::new(opts)?;
    let instances = DbrInstance::fetch_all();
    //let sqlite = sqlite::open(":memory:");

    let val: Vec<String> = conn.query("SHOW TABLES FROM dbr")?;

    //let mut instances = Instance::fetch_all(&mut conn)?;
    /* dbg!(&instances);

       let instance = instances
           .iter()
           .filter(|instance| instance.tag == Some("c1".to_owned()))
           .next()
           .unwrap();
       dbg!(&instance);
       let mut c1_conn = instance.connection()?;
       let values: Vec<mysql::Row> = c1_conn.query("SELECT * FROM customer_order")?;
       dbg!(values);
    */
    Ok(())
}