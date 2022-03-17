use std::collections::{HashSet, HashMap};

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Expr, Ident, Lit, Result, Token, token, Type,
};

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

mod keyword {
    syn::custom_keyword!(and);
    syn::custom_keyword!(or);

    syn::custom_keyword!(order);
    syn::custom_keyword!(by);
    syn::custom_keyword!(asc);
    syn::custom_keyword!(desc);

    syn::custom_keyword!(limit);

    syn::custom_keyword!(like);
    syn::custom_keyword!(not);
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
            Err(lookahead.error())
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
    fn as_sql(&self) -> String {
        match self {
            Self::Eq(_) => "=",
            Self::NotEq(_) => "!=",
            Self::Like(_) => "LIKE",
            Self::NotLike(_, _) => "NOT LIKE",
        }.to_owned()
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
            let inner_group ;
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

                Ok(FilterGroup::Group { paren: None, groups })
            } else {
                Ok(FilterGroup::Expr(initial_expr))
            }
        }
    }
}

impl FilterGroup {
    pub fn expressions(&self) -> Result<Vec<&FilterExpr>> {
        match self {
            FilterGroup::Group {
                paren, groups,
            } => {
                let mut expressions = Vec::new();
                for group in groups {
                    expressions.extend(group.expressions()?);
                }
                Ok(expressions)
            }
            FilterGroup::Expr(expr) => {
                Ok(vec![expr])
            }
        }
    }
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
        self.relations().iter().map(|relation| relation.ident.to_string()).collect()
    }

    pub fn field(&self) -> &FilterPathSegment {
        self.path.segments.iter().last().expect("")
    }

    pub fn field_str(&self) -> String {
        self.field().ident.to_string()
    }
}

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

pub fn fetch(input: FetchInput) -> Result<TokenStream> {
    let table = input.arguments.table;
    let context = input.context;
    let mut filter_path = Vec::new();
    let mut filter_op = Vec::new();
    let mut filter_value = Vec::new();
    let mut filter_format = None;

    if let Some(filter) = input.arguments.filter {
        println!("{:?}", filter.filter_group);
        let expressions = filter.filter_group.expressions()?;
        for expression in expressions {
            let path_str = expression.relations_str();
            let field_str = expression.field_str();

            filter_path.push(quote! {
                RelationPath {
                    path: vec![ #( #path_str ),* ],
                    field: #field_str,
                }
            });
            filter_op.push(expression.op.as_sql());
            filter_value.push(expression.value.clone());
        }

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
    }

    let order_by_str = if let Some(order_by) = input.arguments.order_by {
        order_by.as_sql()
    } else {
        "".to_owned()
    };

    let (limit_str, limit_argument) = if let Some(limit) = input.arguments.limit {
        let limit_expr = limit.limit_expr;
        ("LIMIT ?".to_owned(), Some(quote! {
            arguments.add(#limit_expr);
        }))
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
                let base_table = schema.lookup_table_by_name(#table::table_name().to_owned())?;

                let mut fields = #table::fields();
                let mut joins: Vec<String> = Vec::new();
                let mut filters: Vec<String> = Vec::new();
                let mut arguments = ::sqlx::mysql::MySqlArguments::default();
                let mut relation_chains: Vec<Vec<::rust_dbr::RelationId>> = Vec::new();
                let mut paths: Vec<::rust_dbr::RelationPath> = Vec::new();

                async fn __fetch_recurse(where_tree: WhereTree, context: &::rust_dbr::Context) -> Result<Vec<::rust_dbr::Active<#table>>, ::rust_dbr::DbrError> {
                }

                let mut registry = QueryRegistry::default();
                #( registry.add(context.relation_chain_from_path(#filter_path)?, #filter_op, #filter_value); )*

                let relation_table_count = context.relation_table_count(relation_chains)?;
                for (table, count) in relation_table_count.table_counts {
                    for index in 0..count {

                    }
                }

                #(
                    let chain = context.relation_chain_from_path(#filter_path)?;
                    match RelationChain::as_sql(relation_table_count)? {
                        // if we are colocated we are fine and can just add to the filter list normally.
                        RelationChain::Colocated(filter) => {
                            joins.push(join);
                        }
                        RelationChain::Subquery(subquery) => {

                        }
                    }
                )*

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


/*

        match filter.filter_group {
            FilterGroup::Expr(expr) => {
                let mut segments = expr.path.segments.iter().collect::<Vec<_>>();
                let op = expr.op.as_sql();
                let value_expr = &expr.value;

                // The last portion of a segment is the field.
                let field = segments.pop()
                    .expect("I'm not sure how you managed to get a filter path parsed without a single field...");
                let path = segments;

                let field = field.ident.to_string();
                let path: Vec<String> = path
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect();

                let path = quote! {
                    RelationPath {
                        relations: vec![#( #path.to_owned() ),*].into(),
                        field: #field.to_owned(),
                    }
                };

                filter_paths.push(quote! {
                    paths.push(#path);
                });

                filter_construct.push(quote! {
                    let path = #path;
                });

                (path, op, value_expr)
            }
            _ => {},
        }
*/