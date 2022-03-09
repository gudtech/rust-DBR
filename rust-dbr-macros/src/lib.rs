use syn::{parse_macro_input, DeriveInput};

mod expand;

#[proc_macro_derive(DbrTable, attributes(table, relation))]
pub fn dbr(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand::derive_table::dbr_table(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro]
pub fn fetch(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as expand::fetch::FetchInput);
    expand::fetch::fetch(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
