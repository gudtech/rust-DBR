use sqlx::Type;

use crate::{metadata::{TableId, FieldId}, RelationPath};

pub struct Select {
    filters: FilterTree,
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
    }
}

impl FilterTree {
    /// Remove unnecessary grouping so we don't have to do any unnecessary recursion in the future.
    /// 
    /// Mainly since `A and (B and C)` is semantically the same as `A and B and C`, then we can ungroup `B and C`.
    /// But we cannot reduce `A and (B or C)` into `A and B or C`
    pub fn reduce(self) -> Option<FilterTree> {
        match self {
            or_tree @ Self::Or { .. } => {
                Some(or_tree)
            }
            Self::And { mut children } => {
                match children.len() {
                    0 => None,
                    1 => {
                        let child_tree = children.remove(0);
                        child_tree.reduce()
                    }
                    _ => {
                        let mut new_children = Vec::new();
                        for child in children {
                            if let Some(reduced_child) = child.reduce() {
                                if let Self::And { children: inner_children } = reduced_child {
                                    new_children.extend(inner_children)
                                } else {
                                    new_children.push(reduced_child);
                                }
                            }
                        }

                        Some(Self::And { children: new_children })
                    }
                }
            }
            _ => Some(self)
        }
    }
}