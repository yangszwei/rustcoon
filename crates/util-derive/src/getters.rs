use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Meta, Result, parse_macro_input};

pub fn derive_getters(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match expand_getters(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand_getters(input: DeriveInput) -> Result<proc_macro2::TokenStream> {
    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            Fields::Unnamed(_) | Fields::Unit => {
                return Err(syn::Error::new(
                    ident.span(),
                    "Getters only supports structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new(
                ident.span(),
                "Getters can only be derived for structs",
            ));
        }
    };

    let methods = fields
        .iter()
        .map(|field| {
            let field_ident = field.ident.as_ref().expect("named field missing ident");
            let getter = match GetterOptions::from_attrs(&field.attrs)? {
                Some(getter) => getter,
                None => return Ok(None),
            };
            let docs = field
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("doc"));

            let ty = &field.ty;
            let method = field_ident;
            let (ret_ty, body) = match getter.mode {
                GetterMode::Ref => (quote! { &#ty }, quote! { &self.#field_ident }),
                GetterMode::Copy => (quote! { #ty }, quote! { self.#field_ident }),
                GetterMode::Clone => (quote! { #ty }, quote! { self.#field_ident.clone() }),
            };

            Ok(Some(quote! {
                #(#docs)*
                pub fn #method(&self) -> #ret_ty {
                    #body
                }
            }))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten();

    Ok(quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            #(#methods)*
        }
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GetterMode {
    Ref,
    Copy,
    Clone,
}

#[derive(Clone, Copy, Debug)]
struct GetterOptions {
    mode: GetterMode,
}

impl GetterOptions {
    fn from_attrs(attrs: &[syn::Attribute]) -> Result<Option<Self>> {
        let Some(attr) = attrs.iter().find(|attr| attr.path().is_ident("getter")) else {
            return Ok(None);
        };

        if matches!(attr.meta, Meta::Path(_)) {
            return Ok(Some(Self {
                mode: GetterMode::Copy,
            }));
        }

        let mut options = GetterOptions {
            mode: GetterMode::Copy,
        };
        let mut seen_mode = false;

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("ref") {
                if seen_mode {
                    return Err(meta.error("getter mode already specified"));
                }
                options.mode = GetterMode::Ref;
                seen_mode = true;
                return Ok(());
            }

            if meta.path.is_ident("copy") {
                if seen_mode {
                    return Err(meta.error("getter mode already specified"));
                }
                options.mode = GetterMode::Copy;
                seen_mode = true;
                return Ok(());
            }

            if meta.path.is_ident("clone") {
                if seen_mode {
                    return Err(meta.error("getter mode already specified"));
                }
                options.mode = GetterMode::Clone;
                seen_mode = true;
                return Ok(());
            }

            if seen_mode {
                return Err(meta.error("getter mode already specified"));
            }

            Err(meta.error("expected `ref`, `copy`, or `clone`"))
        })?;

        Ok(Some(options))
    }
}
