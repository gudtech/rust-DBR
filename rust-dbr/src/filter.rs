use std::collections::HashMap;

use sqlx::{any::AnyArguments, encode::IsNull, Type};

//use crate::{metadata::{TableId, FieldId}, RelationPath, Context};
use crate::prelude::*;

type BindValue<'a> = AnyArguments<'a>;

pub enum OrderDirection {
    Ascending,
    Descending,
}

/// This is the construction of a select statement, this must be resolved before being able
/// to be run as a SQL query.
pub struct Select<'a> {
    pub fields: Vec<FieldId>,
    pub primary_table: TableId,
    pub joined_tables: Vec<RelationId>,
    pub filters: Option<FilterTree<'a>>,
    pub order: Vec<(FieldId, Option<OrderDirection>)>,
    pub limit: Option<BindValue<'a>>,
}

/// A resolved select statement.
///
/// This should have enough information by itself to be able to generate a SQL statement and bind arguments.
pub struct ResolvedSelect<'a> {
    pub fields: Vec<Field>,
    pub primary_table: Table,
    pub joins: Vec<(Table, Field, Table, Field)>,
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

        let mut table_registry = TableRegistry::new();
        let resolved_table = context.metadata.lookup_table(primary_table)?.clone();

        let mut resolved_fields = Vec::new();
        for field in fields {
            let field = context.metadata.lookup_field(field)?;
            resolved_fields.push(field.clone());
        }

        let resolved_filters = match filters {
            Some(filters) => {
                Some(filters.resolve(context, resolved_table.id, &mut table_registry)?)
            }
            None => None,
        };

        Ok(ResolvedSelect {
            fields: resolved_fields,
            primary_table: resolved_table,
            joins: Vec::new(),
            filters: resolved_filters,
            order: Vec::new(),
            limit: None,
        })
    }
}

impl<'a> ResolvedSelect<'a> {
    /*
    pub async fn fetch_all<'c, T, E>(self, e: E) -> Result<Vec<T>, DbrError>
        where
            E: sqlx::AnyExecutor<'c>,
            T: for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> + Send + Unpin
    {
        let sql = self.as_sql();
        if let Some((sql, args)) = sql {
            use sqlx::Arguments;

            let result_set: Vec<T> = sqlx::query_as_with(&sql, args)
                .fetch_all(e)
                .await?;

            return Ok(result_set);
        }

        Err(DbrError::Unimplemented(String::new()))
    }
    */

    /// Return pure sql and arguments
    ///
    /// This will return `None` if there is an external subquery somewhere still.
    /// Those have to be run before the "parent" statement.
    pub fn as_sql(self) -> Option<(String, BindValue<'a>)> {
        use sqlx::Arguments;
        let mut arguments = BindValue::default();
        let table = self.primary_table.name.clone();
        let fields = self
            .fields
            .iter()
            .map(|field| format!("{table}.{field}", table = table, field = field.name))
            .collect::<Vec<_>>()
            .join(", ");
        let joins = String::new();
        let (filter_sql, filter_args) = match self.filters {
            Some(filters) => match filters.as_sql() {
                Some((filter_sql, filter_args)) => (filter_sql, filter_args),
                None => return None,
            },
            None => (String::new(), BindValue::default()),
        };

        arguments.extend(filter_args);

        let order = String::new();
        let limit = String::new();
        let database = "account_test";

        Some((
            format!(
                "SELECT {fields} FROM {database}.{table} {joins} {where} {order} {limit}",
                fields = fields,
                database = database,
                table = table,
                joins = joins,
                r#where = filter_sql,
                order = order,
                limit = limit,
            ),
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
                        let table_index = registry.add(context, &current_chain)?;
                        last_table_index = Some(table_index);
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
                    table: from_table.clone(),
                    table_index: last_table_index,
                    field: field.clone(),
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
        table: Table,
        table_index: Option<JoinedTableIndex>,
        field: Field,
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
    pub fn as_sql(self) -> Option<(String, BindValue<'a>)> {
        use sqlx::Arguments;
        match self {
            Self::Or { left, right } => match (left.as_sql(), right.as_sql()) {
                (Some((left_sql, left_args)), Some((right_sql, right_args))) => {
                    let sql = format!("({left} OR {right})", left = left_sql, right = right_sql);
                    let mut args = left_args;
                    args.extend(right_args);
                    Some((sql, args))
                }
                _ => None,
            },
            Self::And { children } => {
                let mut sql = Vec::new();
                let mut args = BindValue::default();
                for child in children {
                    match child.as_sql() {
                        Some((child_sql, child_args)) => {
                            sql.push(child_sql);
                            args.extend(child_args);
                        }
                        _ => return None,
                    }
                }

                Some((sql.join(" AND "), args))
            }
            Self::Predicate(filter) => match filter {
                ResolvedFilter::ExternalSubquery(..) => None,
                ResolvedFilter::Predicate {
                    table,
                    table_index,
                    field,
                    value,
                } => {
                    let table_alias = match table_index {
                        Some(index) => {
                            format!("{table}{index}", table = table.name, index = *index)
                        }
                        _ => table.name.clone(),
                    };

                    let sql = format!(
                        "{table}.{field} = ?",
                        table = table_alias,
                        field = field.name
                    );
                    Some((sql, value))
                }
            },
        }
    }
}
