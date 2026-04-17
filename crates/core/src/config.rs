use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub database_url: String,

    pub redis_url: String,

    #[serde(default = "default_port")]
    pub app_port: u16,

    pub worker_region: String,
    pub worker_hostname: String,

    #[serde(default = "default_check_interval")]
    pub check_interval_secs: u64,

    #[serde(default = "default_log")]
    pub rust_log: String,
}

fn default_port() -> u16 {
    3000
}
fn default_check_interval() -> u64 {
    30
}
fn default_log() -> String {
    "info".to_string()
}

impl Config {
    pub fn load() -> Result<Self, envy::Error> {
        dotenvy::dotenv().ok();
        envy::from_env::<Config>()
    }
}
