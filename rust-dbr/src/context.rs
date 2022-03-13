use std::collections::VecDeque;
use std::sync::Arc;

use crate::{metadata::TableId, prelude::*};

#[derive(Debug, Clone)]
pub struct RelationPath {
    pub relations: VecDeque<String>,
    pub field: String,
}

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

    pub fn lookup_relation_path(
        &self,
        base: TableId,
        mut path: RelationPath,
    ) -> Result<(Vec<&Relation>, String), DbrError> {
        let base_table = self.metadata.lookup_table(base)?;
        let mut relations = Vec::new();
        let filter;

        if let Some(relation_name) = path.relations.pop_front() {
            let relation = base_table.lookup_relation(relation_name)?;

            if relation.len() != 1 {
                return Err(unimplemented!());
            }

            let relation = self.metadata.lookup_relation(relation[0])?;
            relations.push(relation);

            let (recursed_relations, bottom_filter) =
                self.lookup_relation_path(relation.to_table_id, path)?;
            filter = bottom_filter;
            relations.extend(recursed_relations);
        } else {
            filter = format!("{}.{}", base_table.name, path.field)
        }

        Ok((relations, filter))
    }

    pub fn join(&self, relation: &Relation) -> Result<RelationJoin, DbrError> {
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

        if base_instance.info.colocated(&related_instance.info) {
            Ok(RelationJoin::Colocated(format!(
                "JOIN {}.{} ON ({}.{} = {}.{})",
                related_instance.info.database_name(),
                to_table.name,
                to_table.name,
                to_field.name,
                from_table.name,
                from_field.name,
            )))
        } else {
            // we need to do a subquery now.
            Ok(RelationJoin::Subquery("".to_owned()))
        }
    }
}
