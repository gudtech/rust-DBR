fn main() -> Result<(), Box<dyn std::error::Error>> {
    use mysql::*;
    use mysql::prelude::*;

    let opts = Opts::from_url("mysql://devuser:password@localhost:3306/account_test")?;

    let mut conn = Conn::new(opts)?;
    let val: Option<i64> = conn.query_first("SELECT id FROM customer_order")?;
    dbg!(val);
    Ok(())
}
