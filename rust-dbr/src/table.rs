
use crate::prelude::*;

pub trait DbrTable
where
    Self: Send + Sync + Sized + Clone + 'static,
{
    type ActiveModel: ActiveModel<Self>;
    type PartialModel: PartialModel<Self>;
    fn instance_handle() -> &'static str;
    fn table_name() -> &'static str;
}
