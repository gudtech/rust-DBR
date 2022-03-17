use derive_more::Deref;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::{
    metadata::{FieldId, RelationId, TableId},
    prelude::*,
};

#[derive(Debug, Clone)]
pub struct RelationPath {
    pub base: TableId,
    pub relations: VecDeque<String>,
    pub field: String,
}

/// The final portion of a filter in the query calculated from a `RelationPath`
///
/// e.g. `song.name = ?`, `artist.genre = ?`
///
/// In the case that multiple of the same table are joined against then we will have
/// `album.name` and `album2.name`, `artist.genre` and `artist2.genre`
#[derive(Debug, Clone)]
pub struct FilterPath {
    pub table_id: TableId,
    pub table_instance_id: usize,
    pub field_id: FieldId,
}

/// Incrementing count of which joined table instance to use.
#[derive(Deref, Debug, Copy, Clone)]
pub struct JoinedTableIndex(#[deref] u32);

#[derive(Debug, Clone)]
pub enum RelationJoin {
    Subquery(String),
    Colocated(String),
}

#[derive(Clone)]
pub struct Context {
    pub client_id: Option<i64>,
    pub instances: DbrInstances,
    pub metadata: Metadata,
}

#[derive(Deref, Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct QueryId(#[deref] u32);

#[derive(Debug, Clone)]
pub struct TableRegistry {
    instances: HashMap<TableId, JoinedTableIndex>,
    relation_hash: HashMap<RelationChain, JoinedTableIndex>,
}

impl TableRegistry {
    pub fn new() -> Self {
        Self {
            instances: HashMap::new(),
            relation_hash: HashMap::new(),
        }
    }

    pub fn add(&mut self, context: &Context, chain: &RelationChain) -> Result<JoinedTableIndex, DbrError> {
        match self.relation_hash.get(&chain) {
            Some(index) => {
                Ok(*index)
            }
            None => {
                let last_table = if chain.chain.len() > 0 {
                    let last_relation_id = chain.chain[chain.chain.len()-1];
                    let relation = context.metadata.lookup_relation(last_relation_id)?;
                    relation.to_table_id
                } else {
                    chain.base
                };

                dbg!(&chain);
                dbg!(&last_table);
                let instance_count = self
                    .instances
                    .entry(last_table)
                    .or_insert(JoinedTableIndex(0));
                instance_count.0 += 1;
                self.relation_hash.insert(chain.clone(), *instance_count);
                Ok(*instance_count)
            }
        }
    }
}

impl Context {
    pub fn client_id(&self) -> Option<i64> {
        self.client_id
    }

    pub fn client_tag(&self) -> Option<String> {
        self.client_id().map(|client_id| format!("c{}", client_id))
    }

    pub fn instance_by_schema(&self, schema: SchemaId) -> Result<Arc<DbrInstance>, DbrError> {
        self.instances.lookup_by_schema(schema, self.client_tag())
    }

    pub fn instance_by_handle(&self, handle: String) -> Result<Arc<DbrInstance>, DbrError> {
        self.instances.lookup_by_handle(handle, self.client_tag())
    }

    pub fn begin_transaction(&self) -> Context {
        unimplemented!()
    }

    /// I'm taking the liberty of just calling a string of relations like
    pub fn is_colocated(&self, relation: &Relation) -> Result<bool, DbrError> {
        let from_table = self.metadata.lookup_table(relation.from_table_id)?;
        let from_field = self.metadata.lookup_field(relation.from_field_id)?;
        let to_table = self.metadata.lookup_table(relation.to_table_id)?;
        let to_field = self.metadata.lookup_field(relation.to_field_id)?;

        let base_instance = self.instance_by_schema(from_table.schema_id)?;
        let related_instance = self.instance_by_schema(to_table.schema_id)?;

        Ok(base_instance.info.colocated(&related_instance.info))
    }
}

impl RelationPath {
    pub fn into_chain(mut self, context: &Context) -> Result<RelationChain, DbrError> {
        let base_table = context.metadata.lookup_table(self.base)?;
        let mut chain = RelationChain {
            base: base_table.id,
            chain: Vec::new(),
        };

        if let Some(relation_name) = self.relations.pop_front() {
            let relation_ids = base_table.lookup_relation(relation_name)?;

            if relation_ids.len() != 1 {
                // TODO: We should probably have some ability to control which relation we are talking about
                // e.g. if we had 2 different fields relating to the same table.
                //
                // Maybe some syntax like `song.album<parent>.artist` or something?
                // But for now lets just take the first one.
                return Err(unimplemented!());
            }

            let relation_id = relation_ids[0];
            chain.chain.push(relation_id);

            let relation = context.metadata.lookup_relation(relation_id)?;
            chain.chain.extend(self.into_chain(context)?.chain);
        }

        Ok(chain)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationChain {
    base: TableId,
    chain: Vec<RelationId>,
}

impl RelationChain {
    pub fn new(base: TableId) -> Self {
        Self {
            base,
            chain: Vec::new(),
        }
    }

    pub fn push(&mut self, id: RelationId) {
        self.chain.push(id);
    }
}
