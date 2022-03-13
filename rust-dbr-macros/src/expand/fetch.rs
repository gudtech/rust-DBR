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
            Err(syn::Error::new(Span::call_site(), "unexpected token"))
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
            Err(input.error("expected `=`, `!=`, `like`, or `not like`"))
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

#[derive(Debug, Clone)]
pub struct WhereArgs {
    pub keyword: Token![where],
    pub filter_expressions: Punctuated<FilterExpr, keyword::and>,
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
            Err(input.error("expected `asc` or `desc` ordering direction"))
        }
    }
}

#[derive(Debug, Clone)]
pub struct Key {
    ident: Ident,
    direction: Option<OrderByDirection>,
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
    order: keyword::order,
    by: keyword::by,
    keys: Punctuated<Key, Token![,]>,
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

    let table = input.arguments.table;
    let context = input.context;
    let mut filters = Vec::new();

    if let Some(filter) = input.arguments.filter {
        for filter_expression in filter.filter_expressions.iter() {
            let mut segments = filter_expression.path.segments.iter().collect::<Vec<_>>();
            let op = filter_expression.op.as_sql();
            let value_expr = &filter_expression.value;

            // The last portion of a segment is the field.
            let field = segments.pop()
                .expect("I'm not sure how you managed to get a filter path parsed without a single field...");
            let path = segments;

            let field = field.ident.to_string();
            let path: Vec<String> = path
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect();

            filters.push(quote! {
                {
                    let mut path = ::std::collections::VecDeque::new();
                    #(
                        path.push_back(#path.to_owned());
                    )*
                    let (relations, filter) = context.lookup_relation_path(*base_table, RelationPath {
                        relations: path,
                        field: #field.to_owned(),
                    })?;

                    for relation in relations {
                        let joined = context.join(relation)?;
                        if let RelationJoin::Colocated(join) = joined {
                            joins.push(join);
                        }
                    }

                    filters.push(format!("{} {} ?", filter, #op));
                    arguments.add(#value_expr);
                }
            });
        }
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
            async fn __fetch_internal(context: &Context) -> Result<Vec<::rust_dbr::Active<#table>>, ::rust_dbr::DbrError> {
                use ::sqlx::Arguments;

                let instance = context.instance_by_handle(#table::schema().to_owned())?;
                let schema = context
                    .metadata
                    .lookup_schema(SchemaIdentifier::Name(#table::schema().to_owned()))?;
                let base_table = schema.lookup_table_by_name(#table::table_name().to_owned())?;

                let mut fields = #table::fields();
                let mut joins = Vec::new();
                let mut filters = Vec::new();
                //let mut sqlite_arguments = SqliteArguments::new();
                let mut arguments = ::sqlx::mysql::MySqlArguments::default();

                #(
                    #filters
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
                        let filters = format!("WHERE {}", filters.join(" AND "));
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
