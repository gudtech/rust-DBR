pub mod fetch;
pub mod keyword;
pub mod limit;
pub mod order_by;
pub mod r#where;

pub use prelude::*;

pub fn argument_scalar(stream: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    quote::quote! {
        { use ::sqlx::Arguments; let mut args = ::sqlx::any::AnyArguments::default(); args.add(#stream); args }
    }
}

mod prelude {
    pub use super::argument_scalar;

    pub use super::fetch::*;
    pub use super::keyword;
    pub use super::limit::*;
    pub use super::order_by::*;
    pub use super::r#where::*;
}
