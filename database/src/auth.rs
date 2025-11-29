use sqlx::Pool;
use sqlx::postgres::{Postgres, PgPoolOptions};

#[derive(Debug, Clone)]
pub struct Auth {
    pub pool: Pool<Postgres>
}

impl Auth {

    pub async fn new(host: &str, username: &str, password: &str, database: &str) -> Auth {
        let url = format!("postgres://{}:{}@{}/{}", username, password, host, database);
        Auth::new_with_url(&url).await
    }

    pub async fn new_with_url(url: &str) -> Auth {
        let pool = PgPoolOptions::new()
            .max_connections(100)
            .connect(url)
            .await
            .expect(&format!("Error connecting to {}", url));
        Auth { pool }
    }

    pub fn new_with_pool(pool: Pool<Postgres>) -> Auth {
        Auth { pool }
    }

}
