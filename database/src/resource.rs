pub(crate) mod _schema;
pub mod model;
pub mod device;
pub mod group;
pub mod set;
pub mod data;
pub mod buffer;
pub mod slice;

use sqlx::Pool;
use sqlx::postgres::{Postgres, PgPoolOptions};

#[derive(Debug, Clone)]
pub struct Resource {
    pub pool: Pool<Postgres>
}

impl Resource {

    pub async fn new(host: &str, username: &str, password: &str, database: &str) -> Resource {
        let url = format!("postgres://{}:{}@{}/{}", username, password, host, database);
        Resource::new_with_url(&url).await
    }

    pub async fn new_with_url(url: &str) -> Resource {
        let pool = PgPoolOptions::new()
            .max_connections(100)
            .connect(url)
            .await
            .expect(&format!("Error connecting to {}", url));
        Resource { pool }
    }

    pub fn new_with_pool(pool: Pool<Postgres>) -> Resource {
        Resource { pool }
    }

}
