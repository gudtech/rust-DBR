use std::collections::{HashMap, HashSet};

use proc_macro2::{Span, TokenStream};
use quote::quote;
use rust_dbr::filter::FilterTree;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    token, Expr, Ident, Lit, Result, Token, Type,
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

impl WhereArgs {
    pub fn as_filter_tree(&self) -> Result<TokenStream> {
        let expanded = quote! {};

        Ok(expanded)
    }
}

#[derive(Debug, Clone)]
pub enum FilterGroupOp {
    And(keyword::and),
    Or(keyword::or),
}

impl Parse for FilterGroupOp {
    fn parse(input: ParseStream) -> Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(keyword::and) {
            Ok(FilterGroupOp::And(input.parse()?))
        } else if lookahead.peek(keyword::or) {
            Ok(FilterGroupOp::Or(input.parse()?))
        } else {
            Err(lookahead.error())
        }
    }
}

#[derive(Debug, Clone)]
pub enum FilterGroup {
    Group {
        paren: Option<token::Paren>,
        groups: Punctuated<FilterGroup, FilterGroupOp>,
    },
    Expr(FilterExpr),
}

impl Parse for FilterGroup {
    fn parse(input: ParseStream) -> Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(token::Paren) {
            let inner_group;
            let group_paren = syn::parenthesized!(inner_group in input);
            let mut group = FilterGroup::parse(&inner_group)?;
            if let FilterGroup::Group { ref mut paren, .. } = group {
                *paren = Some(group_paren);
            }
            Ok(group)
        } else {
            let initial_expr = FilterExpr::parse(&input)?;
            if FilterGroupOp::parse(&input.fork()).is_ok() {
                let mut groups = Punctuated::new();
                groups.push_value(FilterGroup::Expr(initial_expr));

                loop {
                    if !FilterGroupOp::parse(&input.fork()).is_ok() {
                        break;
                    }
                    let punct = input.parse()?;
                    groups.push_punct(punct);
                    let value = FilterGroup::parse(input)?;
                    groups.push_value(value);
                }

                Ok(FilterGroup::Group {
                    paren: None,
                    groups,
                })
            } else {
                Ok(FilterGroup::Expr(initial_expr))
            }
        }
    }
}

impl FilterGroup {
    pub fn expressions(&self) -> Result<Vec<&FilterExpr>> {
        match self {
            FilterGroup::Group { paren, groups } => {
                let mut expressions = Vec::new();
                for group in groups {
                    expressions.extend(group.expressions()?);
                }
                Ok(expressions)
            }
            FilterGroup::Expr(expr) => Ok(vec![expr]),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterPathSegment {
    pub ident: Ident,
}

impl Parse for FilterPathSegment {
    fn parse(input: ParseStream) -> Result<Self> {
        let ident = input.parse::<Ident>()?;
        Ok(FilterPathSegment { ident })
    }
}

#[derive(Debug, Clone)]
pub struct FilterPath {
    pub segments: Punctuated<FilterPathSegment, Token![.]>,
}

impl Parse for FilterPath {
    fn parse(input: ParseStream) -> Result<Self> {
        let segments = Punctuated::<FilterPathSegment, Token![.]>::parse_separated_nonempty(input)?;
        Ok(FilterPath { segments })
    }
}

#[derive(Debug, Clone)]
pub enum FilterValue {
    Path(FilterPath),
    Lit(Lit),
}
#[derive(Debug, Clone)]
pub struct FilterExpr {
    path: FilterPath,
    op: FilterOp,
    //value: FilterValue,
    value: Expr,
}

impl Parse for FilterExpr {
    fn parse(input: ParseStream) -> Result<Self> {
        let path = input.parse::<FilterPath>()?;
        let op = input.parse::<FilterOp>()?;
        let value = input.parse::<Expr>()?;

        Ok(FilterExpr { path, op, value })
    }
}

impl FilterExpr {
    /// Every portion of the path aside from the field.
    pub fn relations(&self) -> Vec<&FilterPathSegment> {
        let segments = self.path.segments.iter().collect::<Vec<_>>();
        if let Some((field, relations)) = segments.as_slice().split_last() {
            relations.to_vec()
        } else {
            Vec::new()
        }
    }

    pub fn relations_str(&self) -> Vec<String> {
        self.relations()
            .iter()
            .map(|relation| relation.ident.to_string())
            .collect()
    }

    pub fn field(&self) -> &FilterPathSegment {
        self.path.segments.iter().last().expect("")
    }

    pub fn field_str(&self) -> String {
        self.field().ident.to_string()
    }
}

#[derive(Debug, Clone)]
pub enum FilterOp {
    Eq(Token![=]),
    NotEq(Token![!=]),
    Like(keyword::like),
    NotLike(keyword::not, keyword::like),
}

impl FilterOp {
    fn as_sql(&self) -> String {
        match self {
            Self::Eq(_) => "=",
            Self::NotEq(_) => "!=",
            Self::Like(_) => "LIKE",
            Self::NotLike(_, _) => "NOT LIKE",
        }
        .to_owned()
    }
}

impl Parse for FilterOp {
    fn parse(input: ParseStream) -> Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(Token![=]) {
            let eq = input.parse::<Token![=]>()?;
            Ok(FilterOp::Eq(eq))
        } else if lookahead.peek(Token![!=]) {
            let neq = input.parse::<Token![!=]>()?;
            Ok(FilterOp::NotEq(neq))
        } else if lookahead.peek(keyword::like) {
            let like = input.parse::<keyword::like>()?;
            Ok(FilterOp::Like(like))
        } else if lookahead.peek(keyword::not) {
            let not = input.parse::<keyword::not>()?;
            let like = input.parse::<keyword::like>()?;
            Ok(FilterOp::NotLike(not, like))
        } else {
            Err(lookahead.error())
        }
    }
}
