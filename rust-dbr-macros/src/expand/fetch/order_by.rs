
use std::collections::{HashSet, HashMap};

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Expr, Ident, Lit, Result, Token, token, Type,
};

pub use super::prelude::*;

#[derive(Debug, Clone)]
pub struct OrderByArgs {
    pub order: keyword::order,
    pub by: keyword::by,
    pub keys: Punctuated<Key, Token![,]>,
}

impl OrderByArgs {
    pub fn as_sql(&self) -> String {
        let mut keys = Vec::new();
        for key in self.keys.iter() {
            let direction = match &key.direction {
                Some(OrderByDirection::Asc(_)) => " ASC",
                Some(OrderByDirection::Desc(_)) => " DESC",
                None => "",
            };

            keys.push(format!("{}{}", key.ident.to_string(), direction));
        }

        format!("ORDER BY {}", keys.join(", "))
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
