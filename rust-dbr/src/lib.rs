pub mod cache;
pub mod context;
pub mod error;
pub mod filter;
pub mod instance;
pub mod metadata;
pub mod model;
pub mod table;

pub mod prelude {
    pub use crate::cache::{DbrRecordCache, RecordMetadata};
    pub use crate::context::{Context, RelationPath, RelationChain, TableRegistry, JoinedTableIndex};
    pub use crate::error::DbrError;
    pub use crate::instance::{DbrInstance, DbrInstanceId, DbrInstanceInfo, DbrInstances, Pool};
    pub use crate::metadata::{
        Field, FieldId, FieldIdentifier, Metadata, Relation, RelationId, Schema, SchemaId,
        SchemaIdentifier, Table, TableId, TableIdentifier,
    };
    pub use crate::model::{Active, ActiveModel, PartialModel};
    pub use crate::table::DbrTable;
}

pub use prelude::{
    Active, ActiveModel, Context, DbrError, DbrTable, Metadata, PartialModel, Pool, RelationId,
    RelationPath, TableRegistry, SchemaIdentifier, TableIdentifier, RelationChain, JoinedTableIndex,
};
