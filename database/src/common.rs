use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
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

    pub(crate) async fn fetch_count(&self, pool: &Pool<Postgres>) -> Result<usize, Error>
    {
        let count: i64 = sqlx::query_with(&self.query, self.values.clone())
            .map(|row| {
                row.get(0)
            })
            .fetch_one(pool)
            .await?;
        Ok(count as usize)
    }

    pub(crate) async fn fetch_timestamp(&self, pool: &Pool<Postgres>) -> Result<Vec<DateTime<Utc>>, Error>
    {
        let mut rows = sqlx::query_with(&self.query, self.values.clone())
            .map(|row: PgRow| {
                row.get(0)
            })
            .fetch_all(pool)
            .await?;
        rows.dedup();
        Ok(rows)
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

pub mod tag {
    pub const DEFAULT: i16 = 0;
    pub const MINUTELY: i16 = 1;
    pub const MINUTELY_AVG: i16 = 2;
    pub const MINUTELY_MIN: i16 = 3;
    pub const MINUTELY_MAX: i16 = 4;
    pub const HOURLY: i16 = 5;
    pub const HOURLY_AVG: i16 = 6;
    pub const HOURLY_MIN: i16 = 7;
    pub const HOURLY_MAX: i16 = 8;
    pub const DAILY: i16 = 9;
    pub const DAILY_AVG: i16 = 10;
    pub const DAILY_MIN: i16 = 11;
    pub const DAILY_MAX: i16 = 12;
    pub const WEEKLY: i16 = 13;
    pub const WEEKLY_AVG: i16 = 14;
    pub const WEEKLY_MIN: i16 = 15;
    pub const WEEKLY_MAX: i16 = 16;
    pub const MONTHLY: i16 = 17;
    pub const MONTHLY_AVG: i16 = 18;
    pub const MONTHLY_MIN: i16 = 19;
    pub const MONTHLY_MAX: i16 = 20;
    pub const ANNUAL: i16 = 21;
    pub const ANNUAL_AVG: i16 = 22;
    pub const ANNUAL_MIN: i16 = 23;
    pub const ANNUAL_MAX: i16 = 24;
    pub const GROUP_MINUTELY: i16 = 25;
    pub const GROUP_HOURLY: i16 = 26;
    pub const GROUP_DAILY: i16 = 27;
    pub const GROUP_WEEKLY: i16 = 28;
    pub const GROUP_MONTHLY: i16 = 29;
    pub const GROUP_ANNUAL: i16 = 30;
    pub const ERROR: i16 = -1;
    pub const DELETE: i16 = -2;
    pub const HOLD: i16 = -3;
    pub const SEND_UPLINK: i16 = -4;
    pub const SEND_DOWNLINK: i16 = -5;
    pub const TRANSFER_LOCAL: i16 = -6;
    pub const TRANSFER_GATEWAY: i16 = -7;
    pub const TRANSFER_SERVER: i16 = -8;
    pub const BACKUP: i16 = -9;
    pub const RESTORE: i16 = -10;
    pub const ANALYSIS_1: i16 = -11;
    pub const ANALYSIS_2: i16 = -12;
    pub const ANALYSIS_3: i16 = -13;
    pub const ANALYSIS_4: i16 = -14;
    pub const ANALYSIS_5: i16 = -15;
    pub const ANALYSIS_6: i16 = -16;
    pub const ANALYSIS_7: i16 = -17;
    pub const ANALYSIS_8: i16 = -18;
    pub const ANALYSIS_9: i16 = -19;
    pub const ANALYSIS_10: i16 = -20;
    pub const EXTERNAL_INPUT: i16 = -21;
    pub const EXTERNAL_OUTPUT: i16 = -22;
    pub const SUCCESS: i16 = 1;
    pub const ERROR_UNKNOWN: i16 = -1;
    pub const ERROR_LOG: i16 = -2;
    pub const ERROR_SEND: i16 = -3;
    pub const ERROR_TRANSFER: i16 = -4;
    pub const ERROR_ANALYSIS: i16 = -5;
    pub const ERROR_NETWORK: i16 = -6;
    pub const FAIL_READ: i16 = -7;
    pub const FAIL_CREATE: i16 = -8;
    pub const FAIL_UPDATE: i16 = -9;
    pub const FAIL_DELETE: i16 = -10;
    pub const INVALID_TOKEN: i16 = -11;
    pub const INVALID_REQUEST: i16 = -12;
}
