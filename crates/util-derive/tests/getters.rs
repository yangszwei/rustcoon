use std::net::SocketAddr;

use rustcoon_derive::Getters;

#[derive(Debug, Clone, PartialEq, Eq, Getters)]
struct ServerConfig {
    #[getter(ref)]
    bind_addr: SocketAddr,
    #[getter(clone)]
    name: String,
    #[getter]
    retries: u32,
    secret: String,
}

#[test]
fn generates_getters_per_field() {
    let config = ServerConfig {
        bind_addr: SocketAddr::from(([127, 0, 0, 1], 104)),
        name: "rustcoon".to_owned(),
        retries: 3,
        secret: "hidden".to_owned(),
    };

    assert_eq!(config.bind_addr().to_string(), "127.0.0.1:104");
    assert_eq!(config.name(), "rustcoon");
    assert_eq!(config.retries(), 3);
    assert_eq!(config.secret, "hidden");
}
