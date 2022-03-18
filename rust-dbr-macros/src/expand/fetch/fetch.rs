use std::collections::{HashMap, HashSet};

use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    token, Expr, Ident, Lit, Result, Token, Type, spanned::Spanned,
};

pub use super::prelude::*;

#[derive(Debug, Clone)]
pub struct FetchInput {
    pub context: Expr,
    pub comma: Token![,],
    pub arguments: FetchArguments,
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

pub fn fetch(input: FetchInput) -> Result<TokenStream> {
    let table = input.arguments.table;
    let context = input.context;
    //let mut filter_path = Vec::new();

    let base_table_tokens = quote! { __base_table_id };

    let filter = match input.arguments.filter {
        Some(filter) => {
            let tokens = filter.filter_tree.as_filter_tree_tokens(&base_table_tokens);
            Some(tokens)
        }
        None => None,
    };

    let order_by = if let Some(order) = input.arguments.order_by {
        if let Some(tokens) = order.as_tokens() {
            quote! { __select.order = #tokens; }
        } else {
            quote! { }
        }
    } else {
        quote! { }
    };

    let limit = if let Some(limit) = input.arguments.limit {
        let limit_expr = limit.limit_expr;
        let assert_bindable = quote_spanned! { limit_expr.span() =>
            ::rust_dbr::_assert_bindable(#limit_expr);
        };

        let arg_scalar = argument_scalar(quote! { #limit_expr });
        quote! {
            #assert_bindable
            __select.limit = Some(#arg_scalar);
        }
    } else {
        quote! { }
    };

    // check that args are fine.
    let expanded = quote! {
        async {
            let __context = #context;
            use ::sqlx::Arguments;
            let __instance = __context.instance_by_handle(#table::schema().to_owned())?;

            let __schema = __context
                .metadata
                .lookup_schema(::rust_dbr::SchemaIdentifier::Name(#table::schema().to_owned()))?;
            let __base_table_id = __schema.lookup_table_by_name(#table::table_name().to_owned())?;
            let __base_table = __context.metadata.lookup_table(*__base_table_id)?;

            let mut __select = ::rust_dbr::Select::new(*__base_table_id);
            __select.filters = Some(#filter);
            __select.fields = __base_table.fields.values().cloned().collect();
            #order_by
            #limit

            let __resolved_select = __select.resolve(__context)?;
            let (__sql, __args) = __resolved_select.as_sql()?;

            dbg!(&__sql);

            // We have to capture the variables out here.
            let __result_set: Vec<#table> = ::sqlx::query_as_with(&__sql, __args)
                .fetch_all(&__instance.pool).await?;

            let mut active_records: Vec<::rust_dbr::Active<#table>> = Vec::new();
            for record in __result_set {
                let id = record.id;
                let record_ref = __instance.cache.set_record(id, record)?;
                active_records.push(::rust_dbr::Active::<#table>::from_arc(id, record_ref));
            }

            Ok::<Vec<::rust_dbr::Active<#table>>, ::rust_dbr::DbrError>(active_records)
        }
    };

    Ok(TokenStream::from(expanded))
}
