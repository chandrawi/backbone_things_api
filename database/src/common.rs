use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sea_query_binder::SqlxValues;
use rand::{thread_rng, Rng};
use argon2::{Argon2, PasswordHasher, password_hash::SaltString};

#[derive(Debug, Clone)]
pub struct QuerySet {
    pub query: String,
    pub values: SqlxValues
}

impl QuerySet {

    pub(crate) async fn execute(&self, pool: &Pool<Postgres>) -> Result<(), Error>
    {
        sqlx::query_with(&self.query, self.values.clone())
            .execute(pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn fetch_id(&self, pool: &Pool<Postgres>) -> Result<i32, Error>
    {
        let id = sqlx::query(&self.query)
            .map(|row: PgRow| row.get(0))
            .fetch_one(pool)
            .await?;
        Ok(id)
    }

    pub(crate) async fn fetch_max_order(&self, pool: &Pool<Postgres>, default: i32) -> i32
    {
        sqlx::query_with(&self.query, self.values.clone())
            .map(|row: PgRow| row.try_get(0))
            .fetch_one(pool)
            .await
            .unwrap_or(Ok(default))
            .unwrap_or(default)
    }

}

pub(crate) fn hash_password(password: &str) -> Result<String, Error>
{
    let argon2 = Argon2::default();
    let salt = SaltString::generate(&mut thread_rng());
    match argon2.hash_password(password.as_bytes(), &salt) {
        Ok(hash) => Ok(hash.to_string()),
        Err(_) => Err(Error::InvalidArgument(String::from("The password failed to hash")))
    }
}

pub fn generate_access_key() -> Vec<u8>
{
    (0..32).map(|_| thread_rng().gen_range(0..255)).collect()
}

pub fn generate_token_string() -> String
{
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";
    (0..32).map(|_| CHARSET[thread_rng().gen_range(0..64)] as char).collect()
}
