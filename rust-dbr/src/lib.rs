pub mod cache;
pub mod context;
pub mod error;
pub mod instance;
pub mod metadata;
pub mod model;
pub mod table;
pub mod filter;

pub mod prelude {
    pub use crate::cache::{DbrRecordCache, RecordMetadata};
    pub use crate::context::{Context, RelationPath, RelationTableCount};
    pub use crate::error::DbrError;
    pub use crate::instance::{DbrInstance, DbrInstanceId, DbrInstanceInfo, DbrInstances, Pool};
    pub use crate::metadata::{
        Field, FieldIdentifier, Metadata, Relation, Schema, SchemaIdentifier, Table,
        TableIdentifier, RelationId
    };
    pub use crate::model::{Active, ActiveModel, PartialModel};
    pub use crate::table::DbrTable;
}

pub use prelude::{
    Active, ActiveModel, Context, DbrError, DbrTable, Metadata, PartialModel, Pool,
    SchemaIdentifier, TableIdentifier, RelationPath, RelationTableCount, RelationId,
};
