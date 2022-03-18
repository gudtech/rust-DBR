use std::{collections::HashMap, sync::Arc};

use derive_more::Deref;
use sqlx::{any::AnyArguments, encode::IsNull, Type};

//use crate::{metadata::{TableId, FieldId}, RelationPath, Context};
use crate::prelude::*;

type BindValue<'a> = AnyArguments<'a>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

/// This is the construction of a select statement, this must be resolved before being able
/// to be run as a SQL query.
pub struct Select<'a>
{
    pub fields: Vec<FieldId>,
    pub primary_table: TableId,
    pub joined_tables: Vec<RelationId>,
    pub filters: Option<FilterTree<'a>>,
    pub order: Vec<(String, Option<OrderDirection>)>,
    pub limit: Option<BindValue<'a>>,
}

#[derive(Debug, Clone)]
pub struct ResolvedJoin {
    pub length: usize,
    pub from_table: ResolvedTable,
    pub from_field: Field,
    pub from_instance_index: Option<JoinedTableIndex>,
    pub to_table: ResolvedTable,
    pub to_field: Field,
    pub to_instance_index: Option<JoinedTableIndex>,
}

impl ResolvedJoin {
    pub fn as_sql(&self) -> String {
        format!(
            "JOIN {} ON ({}.{} = {}.{})",
            self.to_table.instanced_with_schema(self.to_instance_index),
            self.from_table.instanced(self.from_instance_index),
            self.from_field.name,
            self.to_table.instanced(self.to_instance_index),
            self.to_field.name,
        )
    }
}

impl PartialEq for ResolvedJoin {
    fn eq(&self, other: &Self) -> bool {
        self.from_table == other.from_table
            && self.from_field.id == other.from_field.id
            && self.from_instance_index == other.from_instance_index
            && self.to_table == other.to_table
            && self.to_field.id == other.to_field.id
            && self.to_instance_index == other.to_instance_index
    }
}

#[derive(Deref, Debug, Clone)]
pub struct ResolvedTable {
    pub instance: Arc<DbrInstance>,
    #[deref]
    pub table: Table,
}

impl Table {
    pub fn resolve(&self, context: &Context) -> Result<ResolvedTable, DbrError> {
        let instance = context.instance_by_schema(self.schema_id)?;
        Ok(ResolvedTable {
            instance: instance,
            table: self.clone(),
        })
    }
}

impl ResolvedTable {
    pub fn instanced(&self, instance: Option<JoinedTableIndex>) -> String {
        match instance {
            Some(instance) => {
                format!(
                    "{table}{index}",
                    table = self.table.name,
                    index = instance.to_string()
                )
            }
            _ => self.table.name.clone(),
        }
    }
    pub fn instanced_with_schema(&self, instance: Option<JoinedTableIndex>) -> String {
        format!(
            "{}.{} AS {}",
            self.instance.info.database_name(),
            self.table.name,
            self.instanced(instance),
        )
    }
}

impl PartialEq for ResolvedTable {
    fn eq(&self, other: &Self) -> bool {
        self.table.id == other.table.id
    }
}

/// A resolved select statement.
///
/// This should have enough information by itself to be able to generate a SQL statement and bind arguments.
pub struct ResolvedSelect<'a> {
    pub fields: Vec<Field>,
    pub primary_table: ResolvedTable,
    pub joins: Vec<ResolvedJoin>,
    pub filters: Option<ResolvedFilterTree<'a>>,
    pub order: Vec<(Field, Option<OrderDirection>)>,
    pub limit: Option<BindValue<'a>>,
}

impl<'a> Select<'a> {
    pub fn new(primary_table: TableId) -> Self {
        Select {
            primary_table,
            fields: Vec::new(),
            joined_tables: Vec::new(),
            filters: None,
            order: Vec::new(),
            limit: None,
        }
    }

    pub fn can_be_subquery(&self) -> bool {
        self.fields.len() == 1
    }

    pub fn resolve(self, context: &Context) -> Result<ResolvedSelect<'a>, DbrError> {
        let Select::<'a> {
            fields,
            primary_table,
            joined_tables,
            filters,
            order,
            limit,
        } = self;

        let mut joins = Vec::new();
        let mut table_registry = TableRegistry::new();
        let table = context.metadata.lookup_table(primary_table)?.clone();
        let resolved_table = table.resolve(context)?;

        let mut resolved_fields = Vec::new();
        for field in fields {
            let field = context.metadata.lookup_field(field)?;
            resolved_fields.push(field.clone());
        }

        let resolved_filters = match filters {
            Some(filters) => Some(filters.resolve(context, table.id, &mut table_registry)?),
            None => None,
        };

        let table_instances = table_registry.table_instances();
        for (chain, (from_instance_index, to_instance_index)) in table_instances {
            if let Some(relation_id) = chain.last_relation() {
                let relation = context.metadata.lookup_relation(relation_id)?;
                let from_table = context.metadata.lookup_table(relation.from_table_id)?;
                let from_field = context.metadata.lookup_field(relation.from_field_id)?;
                let to_table = context.metadata.lookup_table(relation.to_table_id)?;
                let to_field = context.metadata.lookup_field(relation.to_field_id)?;

                joins.push(ResolvedJoin {
                    length: chain.len(),
                    from_table: from_table.clone().resolve(context)?,
                    from_field: from_field.clone(),
                    from_instance_index: from_instance_index,
                    to_table: to_table.clone().resolve(context)?,
                    to_field: to_field.clone(),
                    to_instance_index: Some(to_instance_index),
                });
            }
        }
        joins.dedup();

        let mut resolved_order = Vec::new();
        for (field_name, direction) in order.into_iter() {
            let field_id = table.lookup_field(field_name)?;
            let field = context.metadata.lookup_field(*field_id)?;
            resolved_order.push((field.clone(), direction));
        }

        Ok(ResolvedSelect {
            fields: resolved_fields,
            primary_table: resolved_table,
            joins: joins,
            filters: resolved_filters,
            order: resolved_order,
            limit: limit,
        })
    }
}

impl<'a> ResolvedSelect<'a> {
    /// Return pure sql and arguments
    ///
    /// This will return `DbrError::UnresolvedQuery` if there is an external subquery somewhere still.
    /// Those have to be run before the "parent" statement.
    pub fn as_sql(mut self) -> Result<(String, BindValue<'a>), DbrError> {
        use sqlx::Arguments;
        let mut arguments = BindValue::default();
        let schema_table = self.primary_table.instanced_with_schema(None);
        let table = self.primary_table.instanced(None);
        let fields = self
            .fields
            .iter()
            .map(|field| format!("{table}.{field}", table = table, field = field.name))
            .collect::<Vec<_>>()
            .join(", ");
        let (filter_sql, filter_args) = match self.filters {
            Some(filters) => {
                let (filter_sql, filter_args) = filters.as_sql()?;
                (format!("WHERE {}", filter_sql), filter_args)
            }
            None => (String::new(), BindValue::default()),
        };

        let mut joins = Vec::new();
        self.joins.sort_by(|a, b| a.length.cmp(&b.length));
        for join in self.joins {
            joins.push(join.as_sql());
        }

        arguments.extend(filter_args);

        let order_str = if self.order.len() > 0 {
            "ORDER BY ".to_owned() + &self.order
                .iter()
                .map(|(field, dir)| {
                    let dir_str = match dir {
                        Some(OrderDirection::Ascending) => " ASC",
                        Some(OrderDirection::Descending) => " DESC",
                        _ => "",
                    };
                    field.name.clone() + dir_str
                })
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            String::new()
        };

        let limit_str = match self.limit {
            Some(limit) => {
                arguments.extend(limit);
                "LIMIT ?".to_owned()
            }
            _ => String::new(),
        };

        let sql = format!(
            "SELECT {fields} FROM {table} {joins} {where} {order} {limit}",
            fields = fields,
            table = schema_table,
            joins = joins.join(" "),
            r#where = filter_sql,
            order = order_str,
            limit = limit_str,
        ).trim().to_owned();

        Ok((
            sql,
            arguments,
        ))
    }
}

pub enum FilterTree<'a> {
    Or {
        left: Box<FilterTree<'a>>,
        right: Box<FilterTree<'a>>,
    },
    And {
        children: Vec<FilterTree<'a>>,
    },
    Predicate(FilterPredicate<'a>),
}

pub enum FilterOp {
    Eq,
    NotEq,
    Like,
    NotLike,
}

pub struct FilterPredicate<'a> {
    pub path: RelationPath,
    pub op: FilterOp,
    pub value: BindValue<'a>,
}

impl<'a> FilterTree<'a> {
    /// Remove unnecessary grouping so we don't have to do any unnecessary recursion in the future.
    ///
    /// Mainly since `A and (B and C)` is semantically the same as `A and B and C`, then we can ungroup `B and C`.
    /// But we cannot reduce `A and (B or C)` into `A and B or C`
    pub fn reduce(self) -> Option<FilterTree<'a>> {
        match self {
            or_tree @ Self::Or { .. } => Some(or_tree),
            Self::And { mut children } => match children.len() {
                0 => None,
                1 => {
                    let child_tree = children.remove(0);
                    child_tree.reduce()
                }
                _ => {
                    let mut new_children = Vec::new();
                    for child in children {
                        if let Some(reduced_child) = child.reduce() {
                            if let Self::And {
                                children: inner_children,
                            } = reduced_child
                            {
                                new_children.extend(inner_children)
                            } else {
                                new_children.push(reduced_child);
                            }
                        }
                    }

                    Some(Self::And {
                        children: new_children,
                    })
                }
            },
            _ => Some(self),
        }
    }

    /// Resolve the query filters into the current context.
    ///
    /// This mostly includes figuring out what tables we have to join and
    /// to run subqueries if necessary.
    pub fn resolve(
        self,
        context: &Context,
        base_table_id: TableId,
        registry: &mut TableRegistry,
    ) -> Result<ResolvedFilterTree<'a>, DbrError> {
        match self {
            Self::Or { left, right } => Ok(ResolvedFilterTree::Or {
                left: Box::new(left.resolve(context, base_table_id, registry)?),
                right: Box::new(right.resolve(context, base_table_id, registry)?),
            }),
            Self::And { children } => {
                let mut resolved = Vec::new();
                for child in children {
                    resolved.push(child.resolve(context, base_table_id, registry)?);
                }

                Ok(ResolvedFilterTree::And { children: resolved })
            }
            Self::Predicate(expr) => {
                let mut current_chain = RelationChain::new(base_table_id);

                let mut from_table = context.metadata.lookup_table(base_table_id)?;
                let mut last_table_index = None;

                let mut relation_walk = expr.path.relations.into_iter();
                while let Some(to_table_name) = relation_walk.next() {
                    let relation = context.metadata.find_relation(
                        SchemaIdentifier::Id(from_table.schema_id),
                        TableIdentifier::Id(from_table.id),
                        TableIdentifier::Name(to_table_name.to_owned()),
                    )?;

                    let to_table = context.metadata.lookup_table(relation.to_table_id)?;

                    if context.is_colocated(relation)? {
                        current_chain.push(relation.id);

                        // We only really care about the table index at the end of a relation chain.
                        let (_from_index, to_index) = registry.add(context, &current_chain)?;
                        last_table_index = Some(to_index);
                    } else {
                        // we gots to do a subquery weeee
                        let mut subquery = Select::new(to_table.id);
                        let primary_key = to_table
                            .primary_key()
                            .ok_or(DbrError::Unimplemented("missing primary key".to_owned()))?;
                        subquery.fields.push(primary_key);

                        // Collect the rest of the relations and add it as a filter to the subquery, then resolve that.
                        subquery.filters = Some(FilterTree::Predicate(FilterPredicate {
                            path: RelationPath {
                                base: to_table.id,
                                relations: relation_walk.collect(),
                                field: expr.path.field,
                            },
                            op: expr.op,
                            value: expr.value,
                        }));

                        let resolved_subquery = subquery.resolve(context)?;
                        return Ok(ResolvedFilterTree::Predicate(
                            ResolvedFilter::ExternalSubquery(Box::new(resolved_subquery)),
                        ));
                    }

                    from_table = to_table;
                }

                let field_id = from_table.lookup_field(expr.path.field)?;
                let field = context.metadata.lookup_field(*field_id)?;

                let predicate = ResolvedFilter::Predicate {
                    table: from_table.resolve(context)?,
                    table_index: last_table_index,
                    field: field.clone(),
                    op: expr.op,
                    value: expr.value,
                };

                Ok(ResolvedFilterTree::Predicate(predicate))
            }
        }
    }
}

pub enum ResolvedFilter<'a> {
    ExternalSubquery(Box<ResolvedSelect<'a>>),
    Predicate {
        table: ResolvedTable,
        table_index: Option<JoinedTableIndex>,
        field: Field,
        op: FilterOp,
        value: BindValue<'a>,
    },
}

pub enum ResolvedFilterTree<'a> {
    Or {
        left: Box<ResolvedFilterTree<'a>>,
        right: Box<ResolvedFilterTree<'a>>,
    },
    And {
        children: Vec<ResolvedFilterTree<'a>>,
    },
    Predicate(ResolvedFilter<'a>),
}

impl<'a> ResolvedFilterTree<'a> {
    pub fn as_sql(self) -> Result<(String, BindValue<'a>), DbrError> {
        use sqlx::Arguments;
        match self {
            Self::Or { left, right } => {
                let (left_sql, left_args) = left.as_sql()?;
                let (right_sql, right_args) = right.as_sql()?;
                let sql = format!("({left} OR {right})", left = left_sql, right = right_sql);
                let mut args = left_args;
                args.extend(right_args);
                Ok((sql, args))
            }
            Self::And { children } => {
                let mut sql = Vec::new();
                let mut args = BindValue::default();
                for child in children {
                    let (child_sql, child_args) = child.as_sql()?;
                    sql.push(child_sql);
                    args.extend(child_args);
                }

                Ok((sql.join(" AND "), args))
            }
            Self::Predicate(filter) => match filter {
                ResolvedFilter::ExternalSubquery(subquery) => {
                    Err(DbrError::UnfinishedExternalSubquery)
                }
                ResolvedFilter::Predicate {
                    table,
                    table_index,
                    field,
                    op,
                    value,
                } => {
                    let table_alias = table.instanced(table_index);
                    let sql = match op {
                        FilterOp::Eq => {
                            format!(
                                "{table}.{field} = ?",
                                table = table_alias,
                                field = field.name
                            )
                        }
                        FilterOp::NotEq => {
                            format!(
                                "{table}.{field} != ?",
                                table = table_alias,
                                field = field.name
                            )
                        }
                        FilterOp::Like => {
                            format!(
                                "{table}.{field} LIKE ?",
                                table = table_alias,
                                field = field.name
                            )
                        }
                        FilterOp::NotLike => {
                            format!(
                                "{table}.{field} NOT LIKE ?",
                                table = table_alias,
                                field = field.name
                            )
                        }
                    };

                    Ok((sql, value))
                }
            },
        }
    }
}
