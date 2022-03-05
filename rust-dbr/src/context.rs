use crate::query::queryable::DbrInstances;

#[derive(Clone)]
pub struct Context {
    client_id: Option<i64>,
    instances: DbrInstances,
    inner: Rc<Inner>
}
struct Inner{
    is_transaction: bool,
    parent: Option<Context>,
    record_cache: ()
}

pub enum Context {
    Global {

    }
    Transaction {

    }
}


impl Context {
    pub fn client_tag(&self) -> Option<String> {
        self.client_id.map(|client_id| format!("c{}", client_id))
    }
}

impl Context {
  pub fn  begin_transaction(&self) -> Context {
    unimplemented!()
   }
    commit(){

    }
}