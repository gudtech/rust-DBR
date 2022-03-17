use std::collections::{VecDeque, HashMap, HashSet};
use std::sync::Arc;
use derive_more::Deref;

use crate::{metadata::{TableId, RelationId, FieldId}, prelude::*};

#[derive(Debug, Clone)]
pub struct RelationPath {
    pub relations: VecDeque<String>,
    pub field: String,
}


/// Which joined instance of the table should we use.
/// 
/// E.g. if we have 2 different relations that end up at the same table
/// we'll join with `JOIN album ON (album.id = _.album_id)`
/// as well as `JOIN album album2 ON (album2.id = _.album_id)`
/// where the second join is an alias.
#[derive(Debug, Clone)]
pub struct RelationTableCount {
    pub table_counts: HashMap<TableId, JoinedTableId>,
    pub chains: HashMap<Vec<RelationId>, (TableId, JoinedTableId)>
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
#[derive(Debug, Copy, Clone)]
pub struct JoinedTableId(u32);

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

pub struct TableRegistry {
    table_instances: HashMap<TableId, JoinedTableId>,
    relation_hash: HashMap<Vec<RelationId>, (TableId, JoinedTableId)>,
}

impl TableRegistry {
    pub fn new() -> Self {
        Self {
            table_instances: HashMap::new(),
            relation_hash: HashMap::new(),
        }
    }

    pub fn add(&mut self, last_relation: &Relation, chain: &Vec<RelationId>) {
        assert!(chain.len() > 0);
        assert_eq!(Some(last_relation.id), chain.get(chain.len()-1).cloned());

        let last_table = last_relation.to_table_id;
        if !self.relation_hash.contains_key(chain) {
            // we don't have this relation,
            // lets mark it down and give it a unique number so it can be referenced
            // in the `WHERE` clause and such.
            let table_instance_count = self.table_instances.entry(last_table).or_insert(JoinedTableId(0));
            table_instance_count.0 += 1;
            self.relation_hash.insert(chain.clone(), (last_table, *table_instance_count));
        }
    }
}

pub struct QueryRegistry<'a> {
    queries: HashMap<QueryId, TableRegistry>,

    // Every time the relation is disconnected, we need another query.
    relation_to_query: HashMap<Vec<RelationId>, QueryId>,
    query_count: u32,
    context: &'a Context,
}

impl<'a> QueryRegistry<'a> {
    pub fn new(context: &'a Context) -> Self {
        let mut registry = Self {
            queries: HashMap::new(),
            relation_to_query: HashMap::new(),
            query_count: 0,
            context,
        };

        registry.add_query();
        registry
    }

    pub fn add_query(&mut self) -> QueryId {
        self.query_count += 1;
        let new_id = QueryId(self.query_count);
        self.queries.insert(new_id, TableRegistry::new());
        new_id
    }

    /// Figure out a consensus on which joins should be used for which relations.
    pub fn add_relation_chain(&mut self, relation_chain: Vec<RelationId>) -> Result<(), DbrError> {
        // We need to reduce relations -> table instances.
        // For example we might have 2 relations to the same table
        // or if we have multiple relations that converge to the same table
        // then we need to differentiate.

        for index in 0..relation_chain.len() {
            // Walk up the relation id chain.
            let mut query_id = QueryId(0);
            if index > 0 {
                // Start with whichever query the previous relation was on.
                let previous_chain = relation_chain[0..index - 1].to_vec();
                if let Some(previous_query_id) = self.relation_to_query.get(&previous_chain) {
                    query_id = *previous_query_id;
                }
            }

            let current_chain = relation_chain[0..index].to_vec();
            let last_relation = self.context.metadata.lookup_relation(relation_chain[index])?;
            
            // The query is not on the same host so we need to do a subquery.
            if !self.context.is_colocated(last_relation)? {
                query_id = self.add_query();
            }

            let table_registry = self.queries.get_mut(&query_id).expect("query somehow didn't exist?");
            table_registry.add(last_relation, &current_chain);
            self.relation_to_query.insert(current_chain, query_id);
        }

        Ok(())
    }
}

impl Context {
    pub fn client_id(&self) -> Option<i64> {
        self.client_id
    }

    pub fn client_tag(&self) -> Option<String> {
        self.client_id().map(|client_id| format!("c{}", client_id))
    }

    pub fn instance_by_handle(&self, handle: String) -> Result<Arc<DbrInstance>, DbrError> {
        self.instances.lookup_by_handle(handle, self.client_tag())
    }

    pub fn begin_transaction(&self) -> Context {
        unimplemented!()
    }

    /// I'm taking the liberty of just calling a string of relations like
    /// song -> album -> artist as a "relation chain" as a part of a "relation tree".
    pub fn relation_chain_from_path(
        &self,
        base: TableId,
        mut path: RelationPath,
    ) -> Result<Vec<RelationId>, DbrError> {
        let base_table = self.metadata.lookup_table(base)?;
        let mut chain = Vec::new();

        if let Some(relation_name) = path.relations.pop_front() {
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
            chain.push(relation_id);

            let relation = self.metadata.lookup_relation(relation_id)?;
            chain.extend(self.relation_chain_from_path(relation.to_table_id, path)?);
        }

        Ok(chain)
    }

    pub fn is_colocated(&self, relation: &Relation) -> Result<bool, DbrError> {
        let from_table = self.metadata.lookup_table(relation.from_table_id)?;
        let from_field = self.metadata.lookup_field(relation.from_field_id)?;
        let from_schema = self
            .metadata
            .lookup_schema(SchemaIdentifier::Id(from_table.schema_id))?;

        let to_table = self.metadata.lookup_table(relation.to_table_id)?;
        let to_field = self.metadata.lookup_field(relation.to_field_id)?;
        let to_schema = self
            .metadata
            .lookup_schema(SchemaIdentifier::Id(to_table.schema_id))?;

        let base_instance = self.instance_by_handle(from_schema.name.to_owned())?;
        let related_instance = self.instance_by_handle(to_schema.name.to_owned())?;

        Ok(base_instance.info.colocated(&related_instance.info))
    }
}
