use rust_dbr::query::queryable::DbrObject;



pub struct Test;

pub trait DbrObjectTest {
    fn test() {

    }
}

impl DbrObjectTest for DbrObject<Test> {
}

pub fn main() {
}