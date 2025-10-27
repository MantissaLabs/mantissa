impl Default for Config {
    fn default() -> Self {
        Config {
            listen_addr: "127.0.0.1:6578".to_string(),
            join_token: String::from(""),
            anchors: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub listen_addr: String,
    #[allow(dead_code)]
    pub join_token: String,
    #[allow(dead_code)]
    pub anchors: Vec<String>,
}

impl Config {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_listen_addr(&mut self, addr: String) -> &mut Config {
        self.listen_addr = addr;
        self
    }

    #[allow(dead_code)]
    pub fn with_join_token(&mut self, token: String) -> &mut Config {
        self.join_token = token;
        self
    }

    pub fn build(&mut self) -> Config {
        self.clone()
    }
}
