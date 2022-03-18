use crate::prelude::*;
use std::fmt::Debug;
use std::hash::Hash;

pub trait DbrTable
where
    Self: Debug + Send + Sync + Sized + Clone + 'static,
{
    type Id: Debug
        + Send
        + Sync
        + Sized
        + Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + 'static;
    type ActiveModel: ActiveModel<Self>;
    type PartialModel: PartialModel<Self>;
    fn schema() -> &'static str;
    fn table_name() -> &'static str;
    fn fields() -> Vec<&'static str>;
}
