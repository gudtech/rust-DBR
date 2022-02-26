use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream, Parser, Result};
use syn::{parse, parse_macro_input, punctuated::Punctuated, Ident, ItemStruct, LitStr, Token};

#[derive(Debug)]
struct Args {
    //pub table_name: LitStr,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Args {
            //table_name: "test".into(),
        })
    }
}

#[proc_macro_attribute]
pub fn dbr(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item_struct = parse_macro_input!(input as ItemStruct);
    let mut args = parse_macro_input!(args as Args);

    if let syn::Fields::Named(ref mut fields) = item_struct.fields {
        fields.named.push(
            syn::Field::parse_named
                .parse2(quote! { pub a: String })
                .unwrap(),
        );
    }

    return quote! {
        #item_struct
    }
    .into();
}
