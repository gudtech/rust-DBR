use std::sync::Arc;

use crate::prelude::*;

#[derive(Clone)]
pub struct Context {
    pub client_id: Option<i64>,
    pub instances: DbrInstances,
    pub pool: mysql_async::Pool,
}

impl Context {
    pub fn client_id(&self) -> Option<i64> {
        self.client_id
    }

    pub fn client_tag(&self) -> Option<String> {
        self.client_id().map(|client_id| format!("c{}", client_id))
    }

    pub fn instance_by_handle(&self, handle: String) -> Option<Arc<DbrInstance>> {
        self.instances.lookup_by_handle(handle, self.client_tag())
    }
}

impl Context {
    pub fn begin_transaction(&self) -> Context {
        unimplemented!()
    }
}