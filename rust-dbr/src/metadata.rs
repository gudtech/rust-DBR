use std::collections::HashMap;

use crate::prelude::*;

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemaId(usize);

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct FieldId(usize);

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableId(usize);

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelationId(usize);

// The metadata is effectively a tree, so lets just have all the data owned
// in the top level with weak reference counted pointers internally.
#[derive(Debug, Clone)]
pub struct Metadata {
    pub schemas: HashMap<SchemaId, Schema>,
    pub tables: HashMap<TableId, Table>,
    pub fields: HashMap<FieldId, Field>,
    pub relations: HashMap<RelationId, Relation>,
}

impl Metadata {
    pub fn fetch(connection: &mut mysql::Conn) -> Result<Self, DbrError> {
        let schemas = Schema::fetch_all(connection)?;
        let tables = Table::fetch_all(connection)?;
        let fields = Field::fetch_all(connection)?;
        let relations = Relation::fetch_all(connection)?;
        Ok(Self::new(schemas, tables, fields, relations))
    }

    pub fn new(
        schema_list: Vec<Schema>,
        table_list: Vec<Table>,
        field_list: Vec<Field>,
        relation_list: Vec<Relation>,
    ) -> Self {
        let mut schemas = HashMap::new();
        let mut tables = HashMap::new();
        let mut fields = HashMap::new();
        let mut relations = HashMap::new();

        for schema in schema_list {
            schemas.insert(schema.id, schema);
        }

        for table in table_list {
            tables.insert(table.id, table);
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
        };

        metadata.build();
        metadata
    }

    // Build internal connections between schemas, tables, fields, and relations.
    pub fn build(&mut self) {
        for (table_id, table) in &self.tables {
            if let Some(schema) = self.schemas.get_mut(&table.schema_id) {
                schema.tables.insert(table.name.clone(), *table_id);
            }
        }

        for (field_id, field) in &self.fields {
            if let Some(table) = self.tables.get_mut(&field.table_id) {
                table.fields.insert(field.name.clone(), *field_id);
            }
        }

        for (relation_id, relation) in &self.relations {
            if let Some(table) = self.tables.get_mut(&relation.from_table_id) {
                table
                    .relations
                    .entry(relation.to_table_id)
                    .or_default()
                    .push(*relation_id);
            }
        }
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
#[derive(Debug, Clone)]
pub struct Schema {
    id: SchemaId,
    name: String, // handle in the dbr.dbr_tables database.
    display_name: String,

    tables: HashMap<String, TableId>,
}

impl Schema {
    pub fn new(id: SchemaId, name: String, display_name: String) -> Self {
        let tables = HashMap::new();
        Self {
            id,
            name,
            display_name,
            tables,
        }
    }

    pub fn fetch_all(metadata: &mut mysql::Conn) -> Result<Vec<Schema>, DbrError> {
        use mysql::prelude::Queryable;

        let schemas = metadata.query_map(
            r"SELECT schema_id, handle, display_name FROM dbr_schemas",
            |(id, name, display_name)| Schema::new(SchemaId(id), name, display_name),
        )?;

        Ok(schemas)
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
#[derive(Debug, Clone)]
pub struct Table {
    id: TableId,
    schema_id: SchemaId,
    name: String,

    fields: HashMap<String, FieldId>,
    relations: HashMap<TableId, Vec<RelationId>>,
}

impl Table {
    pub fn new(id: TableId, schema_id: SchemaId, name: String) -> Self {
        let fields = HashMap::new();
        let relations = HashMap::new();
        Self {
            id,
            schema_id,
            name,
            fields,
            relations,
        }
    }

    pub fn fetch_all(metadata: &mut mysql::Conn) -> Result<Vec<Table>, DbrError> {
        use mysql::prelude::Queryable;

        let tables = metadata.query_map(
            r"SELECT table_id, schema_id, name FROM dbr_tables",
            |(id, schema_id, name)| Table::new(TableId(id), SchemaId(schema_id), name),
        )?;

        Ok(tables)
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
#[derive(Debug, Clone)]
pub struct Field {
    id: FieldId,
    table_id: TableId,
    name: String,
    data_type: u32,
    is_nullable: bool,
    is_signed: bool,
    max_value: u64,
    is_primary_key: bool,
    trans_id: Option<u32>,
    //default_value:
}

impl Field {
    pub fn fetch_all(metadata: &mut mysql::Conn) -> Result<Vec<Field>, DbrError> {
        use mysql::prelude::Queryable;

        let instances = metadata.query_map(
            r"SELECT field_id, table_id, name, data_type, is_nullable, is_signed, max_value, is_pkey, trans_id FROM dbr_fields",
            |(id, table_id, name, data_type, is_nullable, is_signed, max_value, is_primary_key, trans_id)| {
                Field {
                    id: FieldId(id),
                    table_id: TableId(table_id),
                    name,
                    data_type,
                    is_nullable,
                    is_signed,
                    max_value,
                    is_primary_key,
                    trans_id,
                }
        })?;

        Ok(instances)
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
    ManyToOne,
}

#[derive(Debug, Clone)]
pub struct Relation {
    id: RelationId,
    from_table_id: TableId,
    from_field_id: FieldId,
    to_table_id: TableId,
    to_field_id: FieldId,
    kind: RelationType,
}

impl Relation {
    pub fn fetch_all(metadata: &mut mysql::Conn) -> Result<Vec<Relation>, DbrError> {
        use mysql::prelude::Queryable;

        let instances = metadata.query_map(
            r"SELECT relationship_id, from_table_id, from_field_id, to_table_id, to_field_id, type FROM dbr_relationships",
            |(id, from_table_id, from_field_id, to_table_id, to_field_id, kind)| {
                let kind = match kind {
                    2 => RelationType::ManyToOne,
                    _ => return Err(DbrError::Unimplemented("Non-many to one relations".to_owned())),
                };

                Ok(Relation {
                    id: RelationId(id),
                    from_table_id: TableId(from_table_id),
                    from_field_id: FieldId(from_field_id),
                    to_table_id: TableId(to_table_id),
                    to_field_id: FieldId(to_field_id),
                    kind: kind,
                })
        })?;

        Ok(instances
            .into_iter()
            .filter_map(|result| result.ok())
            .collect())
    }
}
