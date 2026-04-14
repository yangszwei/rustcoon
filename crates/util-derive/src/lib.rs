mod getters;
mod new;

use proc_macro::TokenStream;

#[proc_macro_derive(Getters, attributes(getter))]
pub fn derive_getters(input: TokenStream) -> TokenStream {
    getters::derive_getters(input)
}

#[proc_macro_derive(New)]
pub fn derive_new(input: TokenStream) -> TokenStream {
    new::derive_new(input)
}
