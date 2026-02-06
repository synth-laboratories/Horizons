#![cfg(test)]

pub(crate) struct TestInfra {
    pub pg_url: String,
    pub redis_url: String,
}

impl TestInfra {
    /// Integration-style tests require a running Postgres and Redis.
    ///
    /// Set:
    /// - `HORIZONS_TEST_POSTGRES_URL` (e.g. `postgres://postgres:postgres@127.0.0.1:5432/postgres`)
    /// - `HORIZONS_TEST_REDIS_URL` (e.g. `redis://127.0.0.1:6379/`)
    pub(crate) async fn from_env() -> Option<Self> {
        let pg_url = std::env::var("HORIZONS_TEST_POSTGRES_URL").ok()?;
        let redis_url = std::env::var("HORIZONS_TEST_REDIS_URL").ok()?;
        Some(Self { pg_url, redis_url })
    }
}
