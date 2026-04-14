use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Result, parse_macro_input};

pub fn derive_new(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match expand_new(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand_new(input: DeriveInput) -> Result<proc_macro2::TokenStream> {
    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            Fields::Unnamed(_) | Fields::Unit => {
                return Err(syn::Error::new(
                    ident.span(),
                    "New only supports structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new(
                ident.span(),
                "New can only be derived for structs",
            ));
        }
    };

    let params = fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field missing ident");
        let ty = &field.ty;
        quote! { #field_ident: #ty }
    });

    let inits = fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field missing ident");
        quote! { #field_ident }
    });

    Ok(quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            pub fn new(#(#params),*) -> Self {
                Self {
                    #(#inits),*
                }
            }
        }
    })
}
