use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream, Parser, Result};
use syn::{
    parse, parse_macro_input, punctuated::Punctuated, DeriveInput, Ident, ItemStruct, LitStr, Token,
};

#[proc_macro_derive(DbrTable, attributes(table, relation))]
pub fn dbr(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);

    dbg!(&input.attrs);
    let ident = input.ident.clone();
    let partial_ident = format_ident!("Partial{}", input.ident.clone());

    let expanded = quote! {
        pub struct #partial_ident {

        }

        impl ::rust_dbr::prelude::PartialModel<#ident> for #partial_ident {
            fn apply() -> Result<(), DbrError> {
                Ok(())
            }
        }

        impl ::rust_dbr::prelude::DbrTable for #ident {
            type ActiveModel = Active<#ident>;
            type PartialModel = #partial_ident;
            fn instance_handle() -> &'static str {
                "ops"
            }
            fn table_name() -> &'static str {
                "song"
            }
        }

    };

    TokenStream::from(expanded)
}

#[proc_macro]
pub fn fetch(input: TokenStream) -> TokenStream {
    input
}
