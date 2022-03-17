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
pub struct WhereArgs {
    pub keyword: Token![where],
    pub filter_group: FilterTree,
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
pub enum FilterTree {
    Or {
        paren: Option<token::Paren>,
        left: Box<FilterTree>,
        right: Box<FilterTree>,
    },
    And {
        paren: Option<token::Paren>,
        and: Punctuated<FilterTree, keyword::and>,
    },
    Expr {
        paren: Option<token::Paren>,
        expr: FilterExpr,
    },
}

impl Parse for FilterTree {
    fn parse(input: ParseStream) -> Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(token::Paren) {
            dbg!("paren");
            let inner_group;
            let group_paren = syn::parenthesized!(inner_group in input);
            let mut group = FilterTree::parse(&inner_group)?;
            let mut unnecessary = false;
            match group {
                Self::Or { ref mut paren, .. } => {
                    *paren = Some(group_paren);
                }
                Self::And { ref mut paren, .. } => {
                    *paren = Some(group_paren);
                    unnecessary = true;
                }
                Self::Expr { ref mut paren, .. } => {
                    *paren = Some(group_paren);
                    unnecessary = true;
                }
            }

            if unnecessary {
                group_paren
                    .span
                    .unwrap()
                    .warning("unnecessary parens")
                    .emit();
            }

            return Ok(group);
        }

        let expr = FilterTree::Expr {
            paren: None,
            expr: input.parse()?,
        };

        let lookahead = input.lookahead1();
        if lookahead.peek(keyword::and) {
            dbg!("and");
            let mut punctuated = Punctuated::default();
            punctuated.push_value(expr);

            let and: keyword::and = input.parse()?;
            punctuated.push_punct(and);

            loop {
                punctuated.push_value(input.parse()?);
                let lookahead = input.lookahead1();
                if !lookahead.peek(keyword::and) {
                    break;
                }

                punctuated.push_punct(input.parse()?);
            }

            Ok(FilterTree::And {
                paren: None,
                and: punctuated,
            })
        } else if lookahead.peek(keyword::or) {
            dbg!("or");
            Ok(FilterTree::Or {
                paren: None,
                left: Box::new(expr),
                right: Box::new(input.parse()?),
            })
        } else {
            dbg!("expr");
            Ok(expr)
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

impl FilterPath {
    /// Every portion of the path aside from the field.
    pub fn relations(&self) -> Vec<&FilterPathSegment> {
        let segments = self.segments.iter().collect::<Vec<_>>();
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
        self.segments.iter().last().expect("")
    }

    pub fn field_str(&self) -> String {
        self.field().ident.to_string()
    }

    pub fn as_relation_path_tokens(&self, base_table_expr: Expr) -> TokenStream {
        let relations_str = self.relations_str();
        let field_str = self.field_str();
        quote! {
            RelationPath {
                base_table: #base_table_expr,
                path: vec![ #( #relations_str ),* ],
                field: #field_str,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterExpr {
    path: FilterPath,
    op: FilterOp,
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
    pub fn as_filter_tokens(&self, base_table_expr: Expr) -> TokenStream {
        let op_tokens = self.op.as_tokens();
        let path_tokens = self.path.as_relation_path_tokens(base_table_expr);
        let value_tokens = &self.value;
        quote! {
            FilterExpr {
                path: #path_tokens,
                op: #op_tokens,
                value: { use ::sqlx::Arguments; let mut args = ::sqlx::any::AnyArguments::default() args.add(#value_tokens); args },
            }
        }
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
    pub fn as_tokens(&self) -> TokenStream {
        match self {
            Self::Eq(_) => quote! { ::rust_dbr::FilterOp::Eq },
            Self::NotEq(_) => quote! { ::rust_dbr::FilterOp::NotEq },
            Self::Like(_) => quote! { ::rust_dbr::FilterOp::Like },
            Self::NotLike(_, _) => quote! { ::rust_dbr::FilterOp::NotLike },
        }
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
