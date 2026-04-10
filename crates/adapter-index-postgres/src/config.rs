#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresCatalogConfig {
    connection_string: String,
    max_connections: u32,
}

impl PostgresCatalogConfig {
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            max_connections: 10,
        }
    }

    pub fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections.max(1);
        self
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    pub fn max_connections(&self) -> u32 {
        self.max_connections
    }
}

#[cfg(test)]
mod tests {
    use super::PostgresCatalogConfig;

    #[test]
    fn config_builder_clamps_pool_size() {
        let config =
            PostgresCatalogConfig::new("postgres://localhost/rustcoon").with_max_connections(0);

        assert_eq!(config.connection_string(), "postgres://localhost/rustcoon");
        assert_eq!(config.max_connections(), 1);
    }
}
