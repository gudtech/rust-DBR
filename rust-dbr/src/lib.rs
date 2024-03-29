pub mod cache;
pub mod context;
pub mod error;
pub mod filter;
pub mod instance;
pub mod metadata;
pub mod model;
pub mod table;

pub fn _assert_bindable<
    'a,
    T: std::marker::Send + ::sqlx::Encode<'a, ::sqlx::MySql> + ::sqlx::Type<::sqlx::MySql>,
>(
    _t: T,
) {
    // just here for compiler errors.
}

pub mod prelude {
    pub use crate::cache::{DbrRecordCache, RecordMetadata};
    pub use crate::context::{
        Context, JoinedTableIndex, RelationChain, RelationPath, TableRegistry,
    };
    pub use crate::error::DbrError;
    pub use crate::filter::{FilterOp, FilterPredicate, FilterTree, OrderDirection, Select};
    pub use crate::instance::{DbrInstance, DbrInstanceId, DbrInstanceInfo, DbrInstances};
    pub use crate::metadata::{
        Field, FieldId, FieldIdentifier, Metadata, Relation, RelationId, Schema, SchemaId,
        SchemaIdentifier, Table, TableId, TableIdentifier,
    };
    pub use crate::model::{Active, ActiveModel, PartialModel};
    pub use crate::table::DbrTable;
}

pub use prelude::{
    Active, ActiveModel, Context, DbrError, DbrTable, FilterOp, FilterPredicate, FilterTree,
    JoinedTableIndex, Metadata, OrderDirection, PartialModel, RelationChain, RelationId,
    RelationPath, SchemaIdentifier, Select, TableIdentifier, TableRegistry,
};
