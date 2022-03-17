use crate::prelude::DbrInstanceId;

#[derive(Debug)]
pub enum DbrError {
    DowncastError,
    Unimplemented(String),
    PoisonError,
    UnregisteredType,
    CannotSetID,
    RecordNotFetched(i64),
    MissingStore(String),
    SqlxError(sqlx::Error),
    PoolDisconnected,
    MissingInstance {
        id: Option<DbrInstanceId>,
        handle: Option<String>,
        tag: Option<String>,
    },
    MetadataError(crate::metadata::MetadataError),
    UnfinishedExternalSubquery,
}

impl std::fmt::Display for DbrError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Self::DowncastError => write!(f, "downcast error"),
            Self::PoisonError => write!(f, "poisoned"),
            Self::UnregisteredType => write!(f, "tried to read unregistered type"),
            Self::CannotSetID => write!(
                f,
                "setting the id of an active record is currently not allowed"
            ),
            Self::RecordNotFetched(id) => write!(f, "record was not available: {}", id),
            Self::Unimplemented(value) => write!(f, "unimplemented {}", value),
            Self::MissingStore(store) => write!(f, "missing store '{}'", store),

            Self::SqlxError(err) => write!(f, "sqlx error: {}", err),
            Self::PoolDisconnected => write!(f, "pool disconnected"),
            Self::MissingInstance { id, handle, tag } => {
                let ident = if let Some(id) = id {
                    format!("{}", id.0)
                } else {
                    "".to_owned()
                };

                let mut extra = "".to_owned();
                if let Some(handle) = handle {
                    extra = format!("{}", handle);
                }

                if let Some(tag) = tag {
                    extra = format!("{}::{}", extra, tag);
                }

                write!(f, "missing instance ({}, {})", ident, extra)
            }
            Self::MetadataError(err) => write!(f, "metadata error: {}", err),
            Self::UnfinishedExternalSubquery => write!(
                f,
                "contains unfinished external subquery, this must be run before the parent"
            ),
        }
    }
}

impl std::error::Error for DbrError {}

impl From<sqlx::Error> for DbrError {
    fn from(err: sqlx::Error) -> Self {
        Self::SqlxError(err)
    }
}

impl From<crate::metadata::MetadataError> for DbrError {
    fn from(err: crate::metadata::MetadataError) -> Self {
        Self::MetadataError(err)
    }
}
