use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    token, Expr, Ident, Result, Token,
};

use super::keyword;

pub use super::prelude::*;

#[derive(Debug, Clone)]
pub struct WhereArgs {
    pub keyword: Token![where],
    pub filter_tree: FilterTree,
}

impl Parse for WhereArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(WhereArgs {
            keyword: input.parse()?,
            filter_tree: input.parse()?,
        })
    }
}

#[derive(Debug, Clone)]
pub enum FilterTree {
    Or {
        paren: Option<token::Paren>,
        or: keyword::or,
        left: Box<FilterTree>,
        right: Box<FilterTree>,
    },
    And {
        paren: Option<token::Paren>,
        and: Punctuated<FilterTree, keyword::and>,
    },
    Predicate {
        paren: Option<token::Paren>,
        predicate: FilterPredicate,
    },
}

impl FilterTree {
    /// Mostly just used to test all the binding values to see if they are encodable
    ///
    /// That way we don't get big scary red squiggly lines,
    /// only small scary red squiggly lines
    pub fn all_predicates(&self) -> Vec<&FilterPredicate> {
        let mut predicates = Vec::new();
        match &self {
            Self::Or { left, right, .. } => {
                predicates.extend(left.all_predicates());
                predicates.extend(right.all_predicates());
            }
            Self::And { and, .. } => {
                for child in and {
                    predicates.extend(child.all_predicates());
                }
            }
            Self::Predicate { predicate, .. } => {
                predicates.push(predicate);
            }
        }

        predicates
    }
}

impl Parse for FilterTree {
    fn parse(input: ParseStream) -> Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(token::Paren) {
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
                Self::Predicate { ref mut paren, .. } => {
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

        let predicate = FilterTree::Predicate {
            paren: None,
            predicate: input.parse()?,
        };

        let lookahead = input.lookahead1();
        if lookahead.peek(keyword::and) {
            let mut punctuated = Punctuated::default();
            punctuated.push_value(predicate);

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
            Ok(FilterTree::Or {
                paren: None,
                or: input.parse()?,
                left: Box::new(predicate),
                right: Box::new(input.parse()?),
            })
        } else {
            Ok(predicate)
        }
    }
}

impl FilterTree {
    pub fn as_filter_tree_tokens(&self, base_table_expr: &TokenStream) -> TokenStream {
        match self {
            Self::And { and, .. } => {
                let mut and_tokens = Vec::new();
                for tree in and {
                    and_tokens.push(tree.as_filter_tree_tokens(base_table_expr));
                }

                quote! {
                    ::rust_dbr::FilterTree::And { children: vec![#(#and_tokens),*], }
                }
            }
            Self::Or { left, right, .. } => {
                let left = left.as_filter_tree_tokens(base_table_expr);
                let right = right.as_filter_tree_tokens(base_table_expr);
                quote! {
                    ::rust_dbr::FilterTree::Or { left: Box::new(#left), right: Box::new(#right) }
                }
            }
            Self::Predicate { predicate, .. } => {
                let filter_predicate = predicate.as_filter_tokens(base_table_expr);
                quote! {
                    ::rust_dbr::FilterTree::Predicate(#filter_predicate)
                }
            }
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
        if let Some((_field, relations)) = segments.as_slice().split_last() {
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
        self.segments.iter().last()
            .expect("I didn't think it could be done, congratulations on getting a filter path parsed without a single field...")
    }

    pub fn field_str(&self) -> String {
        self.field().ident.to_string()
    }

    pub fn as_relation_path_tokens(&self, base_table_expr: &TokenStream) -> TokenStream {
        let relations_str = self.relations_str();
        let field_str = self.field_str();
        quote! {
            ::rust_dbr::RelationPath {
                base: *#base_table_expr,
                relations: vec![ #( #relations_str.to_owned() ),* ].into(),
                field: #field_str.to_owned(),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterPredicate {
    pub path: FilterPath,
    pub op: FilterOp,
    pub value: Expr,
}

impl Parse for FilterPredicate {
    fn parse(input: ParseStream) -> Result<Self> {
        let path = input.parse::<FilterPath>()?;
        let op = input.parse::<FilterOp>()?;
        let value = input.parse::<Expr>()?;

        Ok(Self { path, op, value })
    }
}

impl FilterPredicate {
    pub fn as_filter_tokens(&self, base_table_expr: &TokenStream) -> TokenStream {
        let op_tokens = self.op.as_tokens();
        let path_tokens = self.path.as_relation_path_tokens(base_table_expr);
        let value_tokens = &self.value;
        let arg_scalar = argument_scalar(quote! { #value_tokens });
        quote! {
            ::rust_dbr::FilterPredicate {
                path: #path_tokens,
                op: #op_tokens,
                value: #arg_scalar,
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
