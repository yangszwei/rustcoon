use rustcoon_derive::New;

#[derive(Debug, Clone, PartialEq, Eq, New)]
struct ServerAddress {
    host: String,
    port: u16,
}

#[test]
fn generates_new_constructor() {
    let address = ServerAddress::new("127.0.0.1".to_owned(), 104);

    assert_eq!(
        address,
        ServerAddress {
            host: "127.0.0.1".to_owned(),
            port: 104,
        }
    );
}
