
#[derive(Debug)]
pub enum DbrError {
    DowncastError,
    Unimplemented(String),
    PoisonError,
    UnregisteredType,
    RecordNotFetched(i64),
    MissingStore(String),
    MysqlError(mysql::Error),
    MysqlAsyncError(mysql_async::Error),
}
impl std::fmt::Display for DbrError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Self::DowncastError => write!(f, "downcast error"),
            Self::PoisonError => write!(f, "poisoned"),
            Self::UnregisteredType => write!(f, "tried to read unregistered type"),
            Self::RecordNotFetched(id) => write!(f, "record was not available: {}", id),
            Self::Unimplemented(value) => write!(f, "unimplemented {}", value),
            Self::MissingStore(store) => write!(f, "missing store '{}'", store),

            Self::MysqlError(err) => write!(f, "mysql error: {}", err),
            Self::MysqlAsyncError(err) => write!(f, "mysql async error: {}", err),
        }
    }
}

impl std::error::Error for DbrError {}

impl From<mysql::Error> for DbrError {
    fn from(err: mysql::Error) -> Self {
        Self::MysqlError(err)
    }
}

impl From<mysql_async::Error> for DbrError {
    fn from(err: mysql_async::Error) -> Self {
        Self::MysqlAsyncError(err)
    }
}
