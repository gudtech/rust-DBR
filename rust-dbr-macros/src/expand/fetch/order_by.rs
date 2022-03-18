use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Ident, Result, Token,
};

pub use super::prelude::*;

#[derive(Debug, Clone)]
pub struct OrderByArgs {
    pub order: keyword::order,
    pub by: keyword::by,
    pub keys: Punctuated<Key, Token![,]>,
}

pub trait AsStatementToken {
    fn as_tokens(&self) -> TokenStream;
}

impl AsStatementToken for Option<OrderByDirection> {
    fn as_tokens(&self) -> TokenStream {
        match self {
            Some(OrderByDirection::Asc(_)) => {
                quote! { Some(::rust_dbr::OrderDirection::Ascending) }
            }
            Some(OrderByDirection::Desc(_)) => {
                quote! { Some(::rust_dbr::OrderDirection::Descending) }
            }
            None => quote! { None },
        }
    }
}

impl OrderByArgs {
    pub fn as_tokens(&self) -> Option<TokenStream> {
        let key = self
            .keys
            .iter()
            .map(|key| key.ident.to_string())
            .collect::<Vec<_>>();
        let direction = self
            .keys
            .iter()
            .map(|key| key.direction.as_tokens())
            .collect::<Vec<_>>();
        if key.len() > 0 {
            Some(quote! { vec![#( (#key.to_owned(), #direction) ),*] })
        } else {
            None
        }
    }
}

impl Parse for OrderByArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let order = input.parse::<keyword::order>()?;
        let by = input.parse::<keyword::by>()?;
        let keys = Punctuated::<Key, Token![,]>::parse_separated_nonempty(input)?;

        Ok(OrderByArgs { order, by, keys })
    }
}

#[derive(Debug, Clone)]
pub enum OrderByDirection {
    Asc(keyword::asc),
    Desc(keyword::desc),
}

impl Parse for OrderByDirection {
    fn parse(input: ParseStream) -> Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(keyword::asc) {
            let asc = input.parse::<keyword::asc>()?;
            Ok(OrderByDirection::Asc(asc))
        } else if lookahead.peek(keyword::desc) {
            let desc = input.parse::<keyword::desc>()?;
            Ok(OrderByDirection::Desc(desc))
        } else {
            Err(lookahead.error())
        }
    }
}

#[derive(Debug, Clone)]
pub struct Key {
    pub ident: Ident,
    pub direction: Option<OrderByDirection>,
}

impl Parse for Key {
    fn parse(input: ParseStream) -> Result<Self> {
        let ident = input.parse::<Ident>()?;
        let lookahead = input.lookahead1();
        let direction;
        if lookahead.peek(keyword::asc) || lookahead.peek(keyword::desc) {
            direction = Some(input.parse::<OrderByDirection>()?);
        } else {
            direction = None;
        }

        Ok(Key { ident, direction })
    }
}
