pub struct Config {
    pub client_addr: String,
}

pub fn global_config() -> Config {
    Config {
        client_addr: "http://127.0.0.1:50051".to_string(),
    }
}
