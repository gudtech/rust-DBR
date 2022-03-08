use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream, Parser, Result};
use syn::{
    parse, parse_macro_input, punctuated::Punctuated, DeriveInput, Ident, ItemStruct, LitStr, Token,
};
use syn::{Attribute, Error, Expr, Lit, Meta, MetaNameValue};

#[proc_macro_derive(DbrTable, attributes(table, relation))]
pub fn dbr(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand::dbr_table(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

mod expand {
    use proc_macro2::{Span, TokenStream};
    use quote::{format_ident, quote, ToTokens};
    use syn::parse::{Parse, ParseStream, Parser, Result};
    use syn::{
        parse, parse_macro_input, punctuated::Punctuated, DeriveInput, Ident, ItemStruct, LitStr,
        Token,
    };
    use syn::{Attribute, Data, Error, Expr, Fields, Lit, Meta, MetaNameValue, Type};

    const TABLE_ATTRIBUTE_DESCRIPTOR: &'static str = "#[table = \"...\"]";

    fn table_name(attr: Attribute) -> Result<Option<LitStr>> {
        if !attr.path.is_ident("table") {
            return Ok(None);
        }

        match attr.parse_meta()? {
            Meta::NameValue(MetaNameValue {
                lit: Lit::Str(lit_str),
                ..
            }) => Ok(Some(lit_str)),
            _ => {
                let message = format!("expected {}", TABLE_ATTRIBUTE_DESCRIPTOR);
                Err(Error::new_spanned(attr, message))
            }
        }
    }

    pub fn dbr_table(input: DeriveInput) -> Result<TokenStream> {
        let mut tables = Vec::new();
        for attr in input.attrs {
            if let Some(name) = table_name(attr)? {
                tables.push(name)
            }
        }

        let table = match tables.len() {
            0 => {
                return Err(syn::Error::new(
                    Span::call_site(),
                    format!("expected {}", TABLE_ATTRIBUTE_DESCRIPTOR),
                ))
            }
            1 => tables[0].clone(),
            _ => {
                return Err(syn::Error::new(
                    Span::call_site(),
                    format!("duplicate {}", TABLE_ATTRIBUTE_DESCRIPTOR),
                ))
            }
        };

        let (handle, table_name) = match table.value().split_once(".") {
            Some((handle, table_name)) => (handle.to_owned(), table_name.to_owned()),
            None => return Err(syn::Error::new(Span::call_site(), format!("table name invalid: {}, expected \"handle_name.table_name\", e.g. \"ops.customer_order\"", table.value()))),
        };

        let data = match input.data {
            Data::Struct(data) => data,
            _ => {
                return Err(syn::Error::new(
                    Span::call_site(),
                    format!("`DbrTable` derive only supports structs currently"),
                ))
            }
        };

        let fields = match data.fields {
            Fields::Named(named) => named,
            _ => {
                return Err(syn::Error::new(
                    Span::call_site(),
                    format!("`DbrTable` derive currently requires named field structs"),
                ))
            }
        };

        let named_fields = fields.named.clone();
        let mut partial_fields = fields.named.clone();

        for partial_field in &mut partial_fields {
            let ty = partial_field.ty.to_token_stream();
            let wrapped_ty = quote! { Option<#ty> };
            partial_field.ty = Type::Verbatim(wrapped_ty);
        }

        let vis = input.vis;
        let ident = input.ident.clone();
        let partial_ident = format_ident!("Partial{}", input.ident.clone());
        let fields_trait = format_ident!("{}Fields", input.ident.clone());

        let field_name: Vec<_> = named_fields
            .iter()
            .map(|field| field.ident.clone().expect("field to have name"))
            .collect();
        let field_type: Vec<_> = named_fields.iter().map(|field| field.ty.clone()).collect();

        let getter_fields: Vec<_> = named_fields
            .iter()
            .filter(|field| field.ident.clone().expect("field to have name").to_string() != "id")
            .collect();
        let getter_field_name: Vec<_> = getter_fields
            .iter()
            .map(|field| field.ident.clone().expect("field to have name"))
            .collect();
        let getter_field_type: Vec<_> =
            getter_fields.iter().map(|field| field.ty.clone()).collect();

        let setter_fields = getter_fields.clone();
        let settable_field_name: Vec<_> = setter_fields
            .iter()
            .map(|field| field.ident.clone().expect("field to have a name"))
            .collect();
        let setter_field_fn: Vec<_> = setter_fields
            .iter()
            .map(|field| format_ident!("set_{}", field.ident.clone().expect("field to have a name")))
            .collect();
        let setter_field_type: Vec<_> =
            setter_fields.iter().map(|field| field.ty.clone()).collect();

        let expanded = quote! {
            #[derive(Debug, Default, Clone)]
            #vis struct #partial_ident {
                #partial_fields
            }

            impl ::rust_dbr::PartialModel<#ident> for #partial_ident {
                fn apply<R>(self, mut record: &mut R) -> Result<(), DbrError>
                where
                    R: ::std::ops::Deref<Target = #ident> + ::std::ops::DerefMut,
                {
                    let #partial_ident {
                        #( #field_name ),*
                    } = self;

                    #(
                        if let Some(#settable_field_name) = #settable_field_name {
                            record.#settable_field_name = #settable_field_name;
                        }
                    )*

                    Ok(())
                }
                fn id(&self) -> Option<i64> {
                    self.id
                }
            }

            impl ::rust_dbr::DbrTable for #ident {
                type ActiveModel = ::rust_dbr::Active<#ident>;
                type PartialModel = #partial_ident;
                fn instance_handle() -> &'static str {
                    #handle
                }
                fn table_name() -> &'static str {
                    #table_name
                }
            }

            #[::async_trait::async_trait]
            #vis trait #fields_trait {
                #(
                    fn #getter_field_name(&self) -> Result<#getter_field_type, ::rust_dbr::DbrError>;
                )*

                async fn set(&mut self, context: &::rust_dbr::Context, partial: #partial_ident) -> Result<(), DbrError>;

                #(
                    async fn #setter_field_fn<T: Into<#setter_field_type> + Send>(
                        &mut self,
                        context: &::rust_dbr::Context,
                        #settable_field_name: T,
                    ) -> Result<(), ::rust_dbr::DbrError>;
                )*
            }

            #[::async_trait::async_trait]
            impl #fields_trait for ::rust_dbr::Active<#ident> {
                #(
                    fn #getter_field_name(&self) -> Result<#getter_field_type, ::rust_dbr::DbrError> {
                        let snapshot = self.snapshot()?;
                        Ok(snapshot.#getter_field_name)
                    }
                )*

                async fn set(&mut self, context: &Context, partial: #partial_ident) -> Result<(), ::rust_dbr::DbrError> {
                    let mut connection = context.pool.get_conn().await?;
                    let mut params = ::std::collections::HashMap::<String, mysql_async::Value>::new();
                    let mut set_fields = Vec::new();
                    params.insert("id".to_owned(), self.id().into());

                    let partial_clone = partial.clone();

                    #(
                        if let Some(#settable_field_name) = partial.#settable_field_name {
                            params.insert(stringify!(#settable_field_name).to_owned(), #settable_field_name.into());
                            set_fields.push(format!("{name} = :{name}", name = stringify!(#settable_field_name)));
                        }
                    )*

                    if params.len() == 0 {
                        return Ok(());
                    }

                    let MYSQL_QUERY = format!(
                        r#"UPDATE {} SET {} WHERE id = :id"#,
                        #table_name,
                        set_fields.join(", ")
                    );

                    use ::mysql_async::{prelude::Queryable};
                    connection
                        .exec::<::mysql_async::Row, _, _>(MYSQL_QUERY, ::mysql_async::Params::Named(params))
                        .await?;
                    self.apply_partial(partial_clone)?;

                    Ok(())
                }

                #(
                    async fn #setter_field_fn<T: Into<#setter_field_type> + Send>(
                        &mut self,
                        context: &::rust_dbr::Context,
                        #settable_field_name: T,
                    ) -> Result<(), ::rust_dbr::DbrError> {
                        self.set(
                            context,
                            #partial_ident {
                                #settable_field_name: Some(#settable_field_name.into()),
                                ..Default::default()
                            },
                        )
                        .await?;
                        Ok(())
                    }
                )*
            }
        };

        Ok(TokenStream::from(expanded))
    }
}

#[proc_macro]
pub fn fetch(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    input
}
