use std::collections::{HashMap, HashSet};

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    token, Expr, Ident, Lit, Result, Token, Type,
};

use super::keyword;

pub use super::prelude::*;

#[derive(Debug, Clone)]
pub struct LimitArgs {
    pub limit: keyword::limit,
    pub limit_expr: Expr,
}

impl Parse for LimitArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let limit = input.parse::<keyword::limit>()?;
        let limit_expr = input.parse::<Expr>()?;

        Ok(LimitArgs { limit, limit_expr })
    }
}
