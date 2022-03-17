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

    let base_table_init = quote!{ let base_table = schema.lookup_table_by_name(#table::table_name().to_owned())?; };
    let base_table_tokens = quote! { base_table };

    let filter = match input.arguments.filter {
        Some(filter) => {
            dbg!(&filter.filter_tree);
            let tokens = filter.filter_tree.as_filter_tree_tokens(&base_table_tokens);
            //let mut tokens = None;
            if let FilterTree::And { and, .. }  = filter.filter_tree {
                let first = and.iter().next().unwrap();
                /*
                if let FilterTree::Expr { expr, ..} = first {
                    tokens = Some(expr.as_filter_tokens(&base_table_tokens));
                } */
                //tokens = Some(first.as_filter_tree_tokens(&base_table_tokens));
            }

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
                #base_table_init

                let base_table_id = *#base_table_tokens;

                let mut fields = #table::fields();
                let mut joins: Vec<String> = Vec::new();
                let mut filters: Vec<String> = Vec::new();
                let mut arguments = ::sqlx::mysql::MySqlArguments::default();
                let mut relation_chains: Vec<Vec<::rust_dbr::RelationId>> = Vec::new();
                let mut paths: Vec<::rust_dbr::RelationPath> = Vec::new();

                let filter = #filter;

                let mut registry = ::rust_dbr::TableRegistry::new();
                let resolved = filter.resolve(context, base_table_id, &mut registry)?;
                match resolved.as_sql() {
                    Some((sql, args)) => { dbg!(sql); }
                    _ => { dbg!("external subquery somewhere"); } 
                };

                #limit_argument

                let result_set: Vec<#table> = match &instance.pool {
                    Pool::MySql(pool) => {
                        let fields_select: Vec<_> = fields
                            .iter()
                            .map(|field| format!("{}.{}", #table::table_name(), field))
                            .collect();
                        let base_name =
                            format!("{}.{}", instance.info.database_name(), #table::table_name());
                        let filters = if filters.len() > 0 {
                            format!("WHERE {}", filters.join(" AND "))
                        } else {
                            "".to_owned()
                        };
                        let query = format!(
                            "SELECT {fields} FROM {table} {join} {where} {order} {limit}",
                            fields = fields_select.join(", "),
                            table = base_name,
                            join = joins.join(" "),
                            r#where = filters,
                            order = #order_by_str,
                            limit = #limit_str,
                        );
                        dbg!(&query);
                        sqlx::query_as_with(&query, arguments)
                            .fetch_all(pool).await?
                    }
                    _ => Vec::new(),
                };

                let mut active_records: Vec<Active<#table>> = Vec::new();
                for record in result_set {
                    let id = record.id;
                    let record_ref = instance.cache.set_record(id, record)?;
                    active_records.push(Active::<#table>::from_arc(id, record_ref));
                }

                Ok(active_records)
            }

            __fetch_internal(#context).await
        }
    };

    Ok(TokenStream::from(expanded))
}
