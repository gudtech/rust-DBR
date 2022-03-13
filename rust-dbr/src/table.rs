use crate::prelude::*;

pub trait DbrTable
where
    Self: Send + Sync + Sized + Clone + 'static,
{
    type ActiveModel: ActiveModel<Self>;
    type PartialModel: PartialModel<Self>;
    fn schema() -> &'static str;
    fn table_name() -> &'static str;
    fn fields() -> Vec<&'static str>;
}
