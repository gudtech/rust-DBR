use std::collections::HashMap;

use derive_more::Deref;
use sqlx::{Executor, MySql};

use crate::prelude::*;

#[derive(Debug, Clone)]
pub enum SchemaIdentifier {
    Id(SchemaId),
    Name(String),
}

#[derive(Debug, Clone)]
pub enum TableIdentifier {
    Id(TableId),
    Name(String),
}

#[derive(Debug, Clone)]
pub enum FieldIdentifier {
    Id(FieldId),
    Name(String),
}

#[derive(Debug, Clone)]
pub enum MissingRelation {
    Id(RelationId),
    Table {
        from_table: TableIdentifier,
        to_table: TableIdentifier,
    },
}

impl std::fmt::Display for MissingRelation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Self::Id(id) => write!(f, "missing relation {}", id.0),
            Self::Table {
                from_table,
                to_table,
            } => {
                write!(
                    f,
                    "missing relation from {:?} to {:?}",
                    from_table, to_table
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum MissingField {
    Id(FieldId),
    Table {
        table: TableIdentifier,
        field: FieldIdentifier,
    },
}

impl std::fmt::Display for MissingField {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Self::Id(id) => write!(f, "missing field {}", id.0),
            Self::Table { table, field } => {
                write!(f, "missing field {:?} from table {:?}", field, table)
            }
        }
    }
}

#[derive(Debug)]
pub enum MetadataError {
    MissingRelation(MissingRelation),
    MissingField(MissingField),
    MissingTable {
        schema: Option<SchemaIdentifier>,
        table: TableIdentifier,
    },
    MissingSchema {
        schema: SchemaIdentifier,
    },
}

impl std::fmt::Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Self::MissingSchema { schema } => {
                write!(f, "missing schema {:?}", schema)
            }
            Self::MissingTable { schema, table } => {
                write!(f, "missing table {:?} from schema {:?}", table, schema)
            }
            Self::MissingField(missing) => {
                write!(f, "{}", missing)
            }
            Self::MissingRelation(missing) => {
                write!(f, "{}", missing)
            }
        }
    }
}

impl std::error::Error for MetadataError {}

#[derive(sqlx::Type, Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[sqlx(transparent)]
pub struct SchemaId(u32);

#[derive(sqlx::Type, Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[sqlx(transparent)]
pub struct FieldId(u32);

#[derive(sqlx::Type, Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[sqlx(transparent)]
pub struct TableId(u32);

#[derive(sqlx::Type, Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[sqlx(transparent)]
pub struct RelationId(i32);

// The metadata is effectively a tree, so lets just have all the data owned
// in the top level with weak reference counted pointers internally.
#[derive(Debug, Clone)]
pub struct Metadata {
    pub schemas: HashMap<SchemaId, Schema>,
    pub tables: HashMap<TableId, Table>,
    pub fields: HashMap<FieldId, Field>,
    pub relations: HashMap<RelationId, Relation>,

    pub named_schemas: HashMap<String, SchemaId>,
}

impl Metadata {
    pub async fn fetch<E>(mut executor: E) -> Result<Self, DbrError>
    where
        for<'c> &'c mut E: Executor<'c, Database = MySql>,
    {
        let schemas = SchemaInfo::fetch_all(&mut executor).await?;
        let tables = TableInfo::fetch_all(&mut executor).await?;
        let fields = Field::fetch_all(&mut executor).await?;
        let relations = Relation::fetch_all(&mut executor).await?;

        Self::build(schemas, tables, fields, relations)
    }

    pub fn build(
        schema_list: Vec<SchemaInfo>,
        table_list: Vec<TableInfo>,
        field_list: Vec<Field>,
        relation_list: Vec<Relation>,
    ) -> Result<Self, DbrError> {
        let mut schemas = HashMap::new();
        let mut tables = HashMap::new();
        let mut fields = HashMap::new();
        let mut relations = HashMap::new();

        let mut named_schemas = HashMap::new();

        for schema in schema_list {
            let name = schema.name.clone();
            let id = schema.id.clone();

            schemas.insert(
                schema.id,
                Schema {
                    info: schema,
                    tables: HashMap::new(),
                },
            );

            named_schemas.insert(name, id);
        }

        for table in table_list {
            tables.insert(
                table.id,
                Table {
                    info: table,
                    primary_key: None,
                    fields: HashMap::new(),
                    relations: HashMap::new(),
                },
            );
        }

        for field in field_list {
            fields.insert(field.id, field);
        }

        for relation in relation_list {
            relations.insert(relation.id, relation);
        }

        let mut metadata = Self {
            schemas,
            tables,
            fields,
            relations,

            named_schemas,
        };

        metadata.rebuild();
        Ok(metadata)
    }

    // Build internal connections between schemas, tables, fields, and relations.
    pub fn rebuild(&mut self) {
        for (table_id, table) in &self.tables {
            if let Some(schema) = self.schemas.get_mut(&table.schema_id) {
                schema.tables.insert(table.name.clone(), *table_id);
            }
        }

        for (field_id, field) in &self.fields {
            if let Some(table) = self.tables.get_mut(&field.table_id) {
                table.fields.insert(field.name.clone(), *field_id);
                if field.is_primary_key {
                    table.primary_key = Some(*field_id);
                }
            }
        }

        for (relation_id, relation) in &self.relations {
            let to_table = self.tables.get(&relation.to_table_id).cloned();

            match (to_table, self.tables.get_mut(&relation.from_table_id)) {
                (Some(to_table), Some(from_table)) => {
                    from_table
                        .relations
                        .entry(to_table.name.clone())
                        .or_default()
                        .push(*relation_id);
                }
                _ => {}
            }
        }
    }

    pub fn lookup_schema(&self, identifier: SchemaIdentifier) -> Result<&Schema, DbrError> {
        let schema = match &identifier {
            SchemaIdentifier::Id(id) => self.schemas.get(&id),
            SchemaIdentifier::Name(name) => self
                .named_schemas
                .get(name)
                .map(|id| self.schemas.get(id))
                .flatten(),
        };

        match schema {
            Some(schema) => Ok(schema),
            None => Err(MetadataError::MissingSchema { schema: identifier }.into()),
        }
    }

    pub fn lookup_table(&self, table_id: TableId) -> Result<&Table, DbrError> {
        self.tables.get(&table_id).ok_or(
            MetadataError::MissingTable {
                schema: None,
                table: TableIdentifier::Id(table_id),
            }
            .into(),
        )
    }

    pub fn lookup_field(&self, field: FieldId) -> Result<&Field, DbrError> {
        self.fields
            .get(&field)
            .ok_or(MetadataError::MissingField(MissingField::Id(field)).into())
    }

    pub fn lookup_relation(&self, relation_id: RelationId) -> Result<&Relation, DbrError> {
        self.relations
            .get(&relation_id)
            .ok_or(MetadataError::MissingRelation(MissingRelation::Id(relation_id)).into())
    }

    pub fn find_relation(
        &self,
        from_schema: SchemaIdentifier,
        from_table: TableIdentifier,
        to_table: TableIdentifier,
    ) -> Result<&Relation, DbrError> {
        let from_table_id = match from_table {
            TableIdentifier::Id(id) => id,
            TableIdentifier::Name(name) => {
                let schema = self.lookup_schema(from_schema)?;
                let from_table_id = schema.lookup_table_by_name(name)?;
                *from_table_id
            }
        };

        let from_table = self.lookup_table(from_table_id)?;

        let to_name = match to_table {
            TableIdentifier::Id(id) => self.lookup_table(id)?.name.clone(),
            TableIdentifier::Name(name) => name,
        };

        let relation_id =
            from_table
                .lookup_relation(to_name)?
                .get(0)
                .ok_or(DbrError::Unimplemented(
                    "Missing relation in table list".to_owned(),
                ))?;

        let relation = self.lookup_relation(*relation_id)?;
        Ok(relation)
    }
}

/*
MySQL [dbr]> select * from dbr_schemas limit 1;
+-----------+--------+---------------+
| schema_id | handle | display_name  |
+-----------+--------+---------------+
|         1 | config | Configuration |
+-----------+--------+---------------+
*/
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct SchemaInfo {
    #[sqlx(rename = "schema_id")]
    pub id: SchemaId,

    #[sqlx(rename = "handle")]
    pub name: String,

    pub display_name: String,
}

#[derive(Deref, Debug, Clone)]
pub struct Schema {
    #[deref]
    pub info: SchemaInfo,

    pub tables: HashMap<String, TableId>,
}

impl SchemaInfo {
    pub fn new(id: SchemaId, name: String, display_name: String) -> Self {
        Self {
            id,
            name,
            display_name,
        }
    }

    pub async fn fetch_all<'c, E: Executor<'c, Database = MySql>>(
        executor: E,
    ) -> Result<Vec<Self>, DbrError> {
        sqlx::query_as(r"SELECT schema_id, handle, display_name FROM dbr_schemas")
            .fetch_all(executor)
            .await
            .map_err(|err| DbrError::from(err))
    }
}

impl Schema {
    pub fn lookup_table_by_name(&self, name: String) -> Result<&TableId, DbrError> {
        match self.tables.get(&name) {
            Some(schema) => Ok(schema),
            None => Err(MetadataError::MissingTable {
                schema: Some(SchemaIdentifier::Name(self.info.name.clone())),
                table: TableIdentifier::Name(name),
            }
            .into()),
        }
    }
}

/*
MySQL [dbr]> select * from dbr_tables limit 1;
+----------+-----------+----------------------+--------------+-------------+
| table_id | schema_id | name                 | display_name | is_cachable |
+----------+-----------+----------------------+--------------+-------------+
|        1 |         1 | attribute_config_map | NULL         |           0 |
+----------+-----------+----------------------+--------------+-------------+
*/
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct TableInfo {
    #[sqlx(rename = "table_id")]
    pub id: TableId,
    pub schema_id: SchemaId,
    pub name: String,
}

#[derive(Deref, Debug, Clone)]
pub struct Table {
    #[deref]
    pub info: TableInfo,

    pub primary_key: Option<FieldId>,
    pub fields: HashMap<String, FieldId>,
    pub relations: HashMap<String, Vec<RelationId>>,
}

impl TableInfo {
    pub fn new(id: TableId, schema_id: SchemaId, name: String) -> Self {
        Self {
            id,
            schema_id,
            name,
        }
    }

    pub async fn fetch_all<'c, E: Executor<'c, Database = MySql>>(
        executor: E,
    ) -> Result<Vec<Self>, DbrError> {
        sqlx::query_as(r"SELECT table_id, schema_id, name FROM dbr_tables")
            .fetch_all(executor)
            .await
            .map_err(|err| DbrError::from(err))
    }

}

impl Table {
    pub fn lookup_field(&self, name: String) -> Result<&FieldId, DbrError> {
        match self.fields.get(&name) {
            Some(field) => Ok(field),
            None => Err(MetadataError::MissingField(MissingField::Table {
                table: TableIdentifier::Name(self.info.name.clone()),
                field: FieldIdentifier::Name(name),
            })
            .into()),
        }
    }

    pub fn lookup_relation(&self, name: String) -> Result<&Vec<RelationId>, DbrError> {
        match self.relations.get(&name) {
            Some(relation) => Ok(relation),
            None => Err(MetadataError::MissingRelation(MissingRelation::Table {
                from_table: TableIdentifier::Name(self.info.name.clone()),
                to_table: TableIdentifier::Name(name),
            })
            .into()),
        }
    }

    pub fn primary_key(&self) -> Option<FieldId> {
        self.primary_key
    }
}
/*
        BigInt    => { id => 1, numeric => 1, bits => 64},

        int       => { id => 2, numeric => 1, bits => 32},
        integer   => { id => 2, numeric => 1, bits => 32}, # duplicate

        mediumint => { id => 3, numeric => 1, bits => 24},
        smallint  => { id => 4, numeric => 1, bits => 16},
        tinyint   => { id => 5, numeric => 1, bits => 8},
        bool      => { id => 6, numeric => 1, bits => 1},
        boolean   => { id => 6, numeric => 1, bits => 1},
        float     => { id => 7, numeric => 1, bits => 'NA'},
        double    => { id => 8, numeric => 1, bits => 'NA'},
        varchar   => { id => 9 },
        char      => { id => 10 },
        text      => { id => 11 },
        mediumtext=> { id => 12 },
        blob      => { id => 13 },
        longblob  => { id => 14 },
        mediumblob=> { id => 15 },
        tinyblob  => { id => 16 },
        enum      => { id => 17 }, # I loathe mysql enums
        decimal   => { id => 18, numeric => 1, bits => 'NA'}, # HERE - may need a little more attention for proper range checking
        datetime  => { id => 19 },
        binary    => { id => 20 },
        varbinary => { id => 21 },
*/

/*
MySQL [dbr]> select * from dbr_fields limit 1;
+----------+----------+------+-----------+-------------+-----------+-----------+--------------+---------+------------+----------+-------+-------------+
| field_id | table_id | name | data_type | is_nullable | is_signed | max_value | display_name | is_pkey | index_type | trans_id | regex | default_val |
+----------+----------+------+-----------+-------------+-----------+-----------+--------------+---------+------------+----------+-------+-------------+
|        1 |        1 | id   |         2 |           0 |         0 |        10 | NULL         |       1 |       NULL |     NULL | NULL  | NULL        |
+----------+----------+------+-----------+-------------+-----------+-----------+--------------+---------+------------+----------+-------+-------------+
 */
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct Field {
    #[sqlx(rename = "field_id")]
    pub id: FieldId,
    pub table_id: TableId,
    pub name: String,
    pub data_type: u32,
    pub is_nullable: bool,
    pub is_signed: bool,
    pub max_value: u64,
    #[sqlx(rename = "is_pkey")]
    pub is_primary_key: bool,
    pub trans_id: Option<u32>,
    //default_value:
}

impl Field {
    pub async fn fetch_all<'c, E: Executor<'c, Database = MySql>>(
        executor: E,
    ) -> Result<Vec<Self>, DbrError> {
        sqlx::query_as(r"SELECT field_id, table_id, name, data_type, is_nullable, is_signed, max_value, is_pkey, trans_id FROM dbr_fields")
            .fetch_all(executor)
            .await
            .map_err(|err| DbrError::from(err))
    }
}

/*
MySQL [dbr]> select * from dbr_relations limit 1;
+-----------------+-----------+---------------+---------------+---------+-------------+-------------+------+
| relation_id | from_name | from_table_id | from_field_id | to_name | to_table_id | to_field_id | type |
+-----------------+-----------+---------------+---------------+---------+-------------+-------------+------+
|               1 | users     |           199 |          1186 | client  |         190 |        1135 |    2 |
+-----------------+-----------+---------------+---------------+---------+-------------+-------------+------+
*/

#[derive(Debug, Clone)]
pub enum RelationType {
    //OneToOne,
//OneToMany,
//ManyToOne,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct Relation {
    #[sqlx(rename = "relationship_id")]
    pub id: RelationId,
    pub from_table_id: TableId,
    pub from_field_id: FieldId,
    pub to_table_id: TableId,
    pub to_field_id: FieldId,
    //kind: RelationType,
}

impl Relation {
    pub async fn fetch_all<'c, E: Executor<'c, Database = MySql>>(
        executor: E,
    ) -> Result<Vec<Self>, DbrError> {
        sqlx::query_as(r"SELECT relationship_id, from_table_id, from_field_id, to_table_id, to_field_id FROM dbr_relationships")
            .fetch_all(executor)
            .await
            .map_err(|err| DbrError::from(err))
    }
}
