fn main() -> Result<(), Box<dyn std::error::Error>> {
    //let sqlite = sqlite::open(":memory:");

    //let val: Vec<String> = conn.query("SHOW TABLES")?;

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

/*
INSERT INTO artist (id, name, genre) VALUES (1, "Delta Sleep", "Math rock");
INSERT INTO album (id, name, artist_id) VALUES (1, "Ghost City", 1);
INSERT INTO song (id, name, album_id) VALUES (1, "After Dark", 1);
INSERT INTO song (id, name, album_id) VALUES (2, "Sultans of Ping", 1);
INSERT INTO song (id, name, album_id) VALUES (3, "Ghost", 1);
INSERT INTO song (id, name, album_id) VALUES (4, "Singlefile", 1);
INSERT INTO song (id, name, album_id) VALUES (5, "Dotwork", 1);
INSERT INTO album (id, name, artist_id) VALUES (2, "Twin Galaxies", 1);
INSERT INTO album (id, name, artist_id) VALUES (3, "Soft Sounds", 1);
INSERT INTO album (id, name, artist_id) VALUES (4, "Spring Island", 1);
INSERT INTO song (id, name, album_id) VALUES (6, "Spring Island", 4);
INSERT INTO song (id, name, album_id) VALUES (7, "The Detail", 4);
INSERT INTO song (id, name, album_id) VALUES (8, "Old Soul", 4);
INSERT INTO song (id, name, album_id) VALUES (9, "View to a Fill", 4);

INSERT INTO artist (id, name, genre) VALUES (2, "Yes", "Progressive rock");
INSERT INTO album (id, name, artist_id) VALUES (5, "Fragile", 2);
INSERT INTO song (id, name, album_id) VALUES (10, "Roundabout", 5);
*/
