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
    pub use crate::context::{
        Context, JoinedTableIndex, RelationChain, RelationPath, TableRegistry,
    };
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
    Active, ActiveModel, Context, DbrError, DbrTable, JoinedTableIndex, Metadata, PartialModel,
    Pool, RelationChain, RelationId, RelationPath, SchemaIdentifier, TableIdentifier,
    TableRegistry,
};
