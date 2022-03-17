use std::collections::HashMap;

use sqlx::Type;

//use crate::{metadata::{TableId, FieldId}, RelationPath, Context};
use crate::{prelude::*};

pub enum OrderDirection {
    Ascending,
    Descending,
}

/// This is the construction of a select statement, this must be resolved before being able
/// to be run as a SQL query.
pub struct Select {
    pub fields: Vec<FieldId>,
    pub primary_table: TableId,
    pub joined_tables: Vec<RelationId>,
    pub filters: Option<FilterTree>,
    pub order: Vec<(FieldId, Option<OrderDirection>)>,
    pub limit: Option<sqlx::any::AnyValue>,
}

/// A resolved select statement.
/// 
/// This should have enough information by itself to be able to generate a SQL statement and bind arguments.
pub struct ResolvedSelect {
    pub fields: Vec<Field>,
    pub primary_table: Table,
    pub joins: Vec<(Table, Field, Table, Field)>,
    pub filters: Option<ResolvedFilterTree>,
    pub order: Vec<(Field, Option<OrderDirection>)>,
    pub limit: Option<sqlx::any::AnyValue>,
}

impl Select {
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

    pub fn resolve(self, context: &Context) -> Result<ResolvedSelect, DbrError> {
        let Select {
            fields, primary_table, joined_tables, filters, order, limit
        } = self;

        let resolved_table = context.metadata.lookup_table(primary_table)?.clone();

        let mut resolved_fields = Vec::new();
        for field in fields {
            let field = context.metadata.lookup_field(field)?;
            resolved_fields.push(field.clone());
        }

        Ok(ResolvedSelect {
            fields: resolved_fields,
            primary_table: resolved_table,
            joins: Vec::new(),
            filters: None,
            order: Vec::new(),
            limit: None,
        })
    }
}

pub enum FilterTree {
    Or {
        left: Box<FilterTree>,
        right: Box<FilterTree>,
    },
    And {
        children: Vec<FilterTree>,
    },
    Filter {
        path: RelationPath,
        value: sqlx::any::AnyValue,
    },
}

impl FilterTree {
    /// Remove unnecessary grouping so we don't have to do any unnecessary recursion in the future.
    ///
    /// Mainly since `A and (B and C)` is semantically the same as `A and B and C`, then we can ungroup `B and C`.
    /// But we cannot reduce `A and (B or C)` into `A and B or C`
    pub fn reduce(self) -> Option<FilterTree> {
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
        base_table_id: TableId,
        context: &Context,
        registry: &mut TableRegistry,
    ) -> Result<ResolvedFilterTree, DbrError> {
        match self {
            Self::Or { left, right } => Ok(ResolvedFilterTree::Or {
                left: Box::new(left.resolve(base_table_id, context, registry)?),
                right: Box::new(right.resolve(base_table_id, context, registry)?),
            }),
            Self::And { children } => {
                let mut resolved = Vec::new();
                for child in children {
                    resolved.push(child.resolve(base_table_id, context, registry)?);
                }

                Ok(ResolvedFilterTree::And { children: resolved })
            }
            Self::Filter { path, value } => {
                let mut from_table = context.metadata.lookup_table(base_table_id)?;
                let mut last_table_index = None;

                let mut current_chain = RelationChain {
                    base: base_table_id,
                    chain: Vec::new(),
                };

                let mut relation_walk = path.relations.into_iter();
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
                        let table_index = registry.add(context, &current_chain);
                        last_table_index = Some(table_index);
                    } else {
                        // we gots to do a subquery weeee
                        let mut subquery = Select::new(to_table.id);
                        let primary_key = to_table.primary_key().ok_or(DbrError::Unimplemented("missing primary key".to_owned()))?;
                        subquery.fields.push(primary_key);

                        // Collect the rest of the relations and add it as a filter to the subquery, then resolve that.
                        subquery.filters = Some(FilterTree::Filter {
                            path: RelationPath {
                                base: to_table.id,
                                relations: relation_walk.collect(),
                                field: path.field,
                            },
                            value: value,
                        });

                        let resolved_subquery = subquery.resolve(context)?;
                        return Ok(ResolvedFilterTree::Filter(ResolvedFilter::ExternalSubquery(Box::new(resolved_subquery))));
                    }

                    from_table = to_table;
                }

                let field_id = from_table.lookup_field(path.field)?;
                let field = context.metadata.lookup_field(*field_id)?;

                let simple = ResolvedFilter::Simple {
                    table: from_table.clone(),
                    table_index: last_table_index,
                    field: field.clone(),
                    value: value,
                };
                Ok(ResolvedFilterTree::Filter(simple))
            }
        }
    }

    /*
    pub fn as_sql(&self) -> String {
        match self {
            Self::Or { left, right } => {
                format!("({left} OR {right})", left = left.as_sql(), right = right.as_sql())
            }
            Self::And { children } => {
                let children_sql = children.iter().map(|child| child.as_sql()).collect::<Vec<_>>();
                children_sql.join(" AND ")
            }
            Self::Filter {
                path,
                value,
            } => {
                format!("{path} = ?", path = path.field)
            }
        }
    }
    */
}

pub enum ResolvedFilter {
    ExternalSubquery(Box<ResolvedSelect>),
    Simple {
        table: Table,
        table_index: Option<JoinedTableIndex>,
        field: Field,
        value: sqlx::any::AnyValue,
    },
}

pub enum ResolvedFilterTree {
    Or {
        left: Box<ResolvedFilterTree>,
        right: Box<ResolvedFilterTree>,
    },
    And {
        children: Vec<ResolvedFilterTree>,
    },
    Filter(ResolvedFilter),
}
