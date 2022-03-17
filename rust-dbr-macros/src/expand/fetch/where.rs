
use std::collections::{HashSet, HashMap};

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Expr, Ident, Lit, Result, Token, token, Type,
};

use super::keyword;

pub use super::prelude::*;

#[derive(Debug, Clone)]
pub struct WhereArgs {
    pub keyword: Token![where],
    pub filter_group: FilterGroup,
}

impl Parse for WhereArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(WhereArgs {
            keyword: input.parse()?,
            filter_group: input.parse()?,
        })
    }
}
