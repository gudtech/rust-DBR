use std::collections::{HashMap, HashSet};

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    token, Expr, Ident, Lit, Result, Token, Type,
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

    let base_table_tokens = quote! { base_table_id };

    let filter = match input.arguments.filter {
        Some(filter) => {
            let tokens = filter.filter_tree.as_filter_tree_tokens(&base_table_tokens);
            Some(tokens)
        }
        None => None,
    };

    //filter.filter_tree.
    // Song where album.artist.genre like "math%" and (album.artist.genre like "%rock%" or album.id = 4)
    // expands to
    //
    // SELECT id FROM other.artist artist1
    // WHERE
    //      genre LIKE ? // "math%"
    //
    // SELECT id FROM other.artist artist1
    // WHERE
    //      genre LIKE ? // "%rock%"
    //
    // SELECT ... FROM account_test.song song1
    // JOIN account_test.album album1 ON (song1.album_id = album1.id)
    // #JOIN other.artist artist1 ON (album1.artist_id = artist1.id)
    // WHERE
    //      album1.artist_id IN (SELECT id FROM other.artist artist1 WHERE artist1.genre LIKE ? // "math%")
    // AND (album1.artist_id IN (SELECT id FROM other.artist artist1 WHERE artist1.genre LIKE ? // "%rock%") OR album1.id = ?) // 4
    //
    //      artist1.genre LIKE ?
    // AND (artist1.genre LIKE ? OR album1.id = ?)
    // AND (album1.id IN (...))

    // we need to have the sql take the table instance from the relation

    let order_by_str = if let Some(order_by) = input.arguments.order_by {
        order_by.as_sql()
    } else {
        "".to_owned()
    };

    let (limit_str, limit_argument) = if let Some(limit) = input.arguments.limit {
        let limit_expr = limit.limit_expr;
        (
            "LIMIT ?".to_owned(),
            Some(quote! {
                arguments.add(#limit_expr);
            }),
        )
    } else {
        ("".to_owned(), None)
    };

    let expanded = quote! {
        {
            async fn __fetch_internal(context: &::rust_dbr::Context) -> Result<Vec<::rust_dbr::Active<#table>>, ::rust_dbr::DbrError> {
                use ::sqlx::Arguments;

                let instance = context.instance_by_handle(#table::schema().to_owned())?;
                let schema = context
                    .metadata
                    .lookup_schema(::rust_dbr::SchemaIdentifier::Name(#table::schema().to_owned()))?;
                let base_table_id = schema.lookup_table_by_name(#table::table_name().to_owned())?;
                let base_table = context.metadata.lookup_table(*base_table_id)?;

                let mut select = ::rust_dbr::Select::new(*base_table_id);
                select.filters = Some(#filter);
                select.fields = base_table.fields.values().cloned().collect();

                let resolved_select = select.resolve(context)?;
                let (sql, args) = resolved_select.as_sql()?;
                dbg!(&sql);
                let result_set: Vec<#table> = ::sqlx::query_as_with(&sql, args)
                    .fetch_all(&instance.pool).await?;

                let mut active_records: Vec<::rust_dbr::Active<#table>> = Vec::new();
                for record in result_set {
                    let id = record.id;
                    let record_ref = instance.cache.set_record(id, record)?;
                    active_records.push(::rust_dbr::Active::<#table>::from_arc(id, record_ref));
                }

                Ok(active_records)
            }

            __fetch_internal(#context).await
        }
    };

    Ok(TokenStream::from(expanded))
}
