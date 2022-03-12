use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Error, Expr, Ident, Lit, LitStr, Result, Token,
};

#[derive(Debug, Clone)]
pub struct FetchInput {
    context: Expr,
    comma: Token![,],
    arguments: FetchArguments,
}

impl Parse for FetchInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let context = input.parse::<Expr>()?;
        let comma = input.parse::<Token![,]>()?;
        let arguments = input.parse::<FetchArguments>()?;

        Ok(FetchInput {
            context,
            comma,
            arguments,
        })
    }
}

#[derive(Debug, Clone)]
pub struct FetchArguments {
    table: Ident,
    filter: Option<WhereArgs>,
    order_by: Option<OrderByArgs>,
    limit: Option<LimitArgs>,
}

impl Parse for FetchArguments {
    fn parse(input: ParseStream) -> Result<Self> {
        let table = input.parse::<Ident>()?;

        let mut filter = None;
        let mut order_by = None;
        let mut limit = None;

        let lookahead = input.lookahead1();
        if lookahead.peek(Token![where]) {
            filter = Some(input.parse::<WhereArgs>()?);
        }

        let lookahead = input.lookahead1();
        if lookahead.peek(keyword::order) {
            order_by = Some(input.parse::<OrderByArgs>()?);
        }

        let lookahead = input.lookahead1();
        if lookahead.peek(keyword::limit) {
            limit = Some(input.parse::<LimitArgs>()?);
        }

        Ok(FetchArguments {
            table,
            filter,
            order_by,
            limit,
        })
    }
}

mod keyword {
    syn::custom_keyword!(and);
    syn::custom_keyword!(order);
    syn::custom_keyword!(by);
    syn::custom_keyword!(limit);
}

#[derive(Debug, Clone)]
pub struct FilterPathSegment {
    ident: Ident,
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

impl Parse for FilterValue {
    fn parse(input: ParseStream) -> Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(Ident) {
            let path = input.parse::<FilterPath>()?;
            Ok(FilterValue::Path(path))
        } else if lookahead.peek(Lit) {
            let lit = input.parse::<Lit>()?;
            Ok(FilterValue::Lit(lit))
        } else {
            Err(syn::Error::new(Span::call_site(), "unexpected token"))
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterExpr {
    path: FilterPath,
    eq: Token![=],
    value: FilterValue,
}

impl Parse for FilterExpr {
    fn parse(input: ParseStream) -> Result<Self> {
        let path = input.parse::<FilterPath>()?;
        let eq = input.parse::<Token![=]>()?;
        let value = input.parse::<FilterValue>()?;

        Ok(FilterExpr { path, eq, value })
    }
}

#[derive(Debug, Clone)]
pub struct WhereArgs {
    keyword: Token![where],
    filter_expressions: Punctuated<FilterExpr, keyword::and>,
}

impl Parse for WhereArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let keyword = input.parse::<Token![where]>()?;
        let filter_expressions =
            Punctuated::<FilterExpr, keyword::and>::parse_separated_nonempty(input)?;

        Ok(WhereArgs {
            keyword,
            filter_expressions,
        })
    }
}

#[derive(Debug, Clone)]
pub struct OrderByArgs {
    order: keyword::order,
    by: keyword::by,
    keys: Punctuated<Ident, Token![,]>,
}

impl Parse for OrderByArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let order = input.parse::<keyword::order>()?;
        let by = input.parse::<keyword::by>()?;
        let keys = Punctuated::<Ident, Token![,]>::parse_separated_nonempty(input)?;

        Ok(OrderByArgs { order, by, keys })
    }
}

#[derive(Debug, Clone)]
pub struct LimitArgs {
    limit: keyword::limit,
    limit_expr: Expr,
}

impl Parse for LimitArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let limit = input.parse::<keyword::limit>()?;
        let limit_expr = input.parse::<Expr>()?;

        Ok(LimitArgs { limit, limit_expr })
    }
}

pub fn fetch(input: FetchInput) -> Result<TokenStream> {
    dbg!(&input.arguments);
    Ok(quote! {Ok::<_, ::rust_dbr::DbrError>(Vec::new())})
}
