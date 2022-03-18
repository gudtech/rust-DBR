use derive_more::Deref;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::{
    metadata::{FieldId, RelationId, TableId},
    prelude::*,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Deref, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JoinedTableIndex(#[deref] u32);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    instances: HashMap<Option<RelationId>, JoinedTableIndex>,
    relation_hash: HashMap<RelationChain, (Option<JoinedTableIndex>, JoinedTableIndex)>,
}

impl TableRegistry {
    pub fn new() -> Self {
        Self {
            instances: HashMap::new(),
            relation_hash: HashMap::new(),
        }
    }

    /// We need this to be in the correct order.
    pub fn table_instances(
        self,
    ) -> Vec<(RelationChain, (Option<JoinedTableIndex>, JoinedTableIndex))> {
        self.relation_hash.into_iter().collect()
    }

    pub fn add(
        &mut self,
        _context: &Context,
        chain: &RelationChain,
    ) -> Result<(Option<JoinedTableIndex>, JoinedTableIndex), DbrError> {
        match self.relation_hash.get(&chain) {
            Some(index) => Ok(*index),
            None => {
                let last_relation = chain.last_relation();
                let previous_chain = chain.previous_chain();
                let previous_index = match self.relation_hash.get(&previous_chain) {
                    Some((_, previous_index)) => Some(*previous_index),
                    _ => None,
                };

                let instance_count = self
                    .instances
                    .entry(last_relation)
                    .or_insert(JoinedTableIndex(0));

                instance_count.0 += 1;

                self.relation_hash
                    .insert(chain.clone(), (previous_index, *instance_count));
                Ok((previous_index, *instance_count))
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
        let to_table = self.metadata.lookup_table(relation.to_table_id)?;

        let base_instance = self.instance_by_schema(from_table.schema_id)?;
        let related_instance = self.instance_by_schema(to_table.schema_id)?;

        Ok(base_instance.info.colocated(&related_instance.info))
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

    pub fn last_relation(&self) -> Option<RelationId> {
        if self.chain.len() > 0 {
            let last_relation_id = self.chain[self.chain.len() - 1];
            Some(last_relation_id)
        } else {
            None
        }
    }

    pub fn previous_chain(&self) -> RelationChain {
        let mut clone = self.clone();
        if clone.chain.len() > 0 {
            // cut off the end part of the chain.
            clone.chain = clone.chain[..clone.chain.len() - 1].to_vec();
        }
        clone
    }

    pub fn len(&self) -> usize {
        self.chain.len()
    }
}
