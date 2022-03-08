use std::sync::Arc;

use crate::prelude::*;

#[derive(Clone)]
pub struct Context {
    client_id: Option<i64>,
    instances: DbrInstances,
}

impl Context {
    pub fn client_tag(&self) -> Option<String> {
        self.client_id.map(|client_id| format!("c{}", client_id))
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