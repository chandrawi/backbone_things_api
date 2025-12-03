use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{SelectStatement, InsertStatement, UpdateStatement, DeleteStatement, PostgresQueryBuilder};
use sea_query_binder::{SqlxBinder, SqlxValues};

#[derive(Debug, Clone)]
pub enum QueryStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement)
}

impl QueryStatement {

    pub fn build(&self) -> (String, SqlxValues) {
        match self {
            Self::Select(stmt) => stmt.build_sqlx(PostgresQueryBuilder),
            Self::Insert(stmt) => stmt.build_sqlx(PostgresQueryBuilder),
            Self::Update(stmt) => stmt.build_sqlx(PostgresQueryBuilder),
            Self::Delete(stmt) => stmt.build_sqlx(PostgresQueryBuilder)
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Self::Select(stmt) => stmt.to_string(PostgresQueryBuilder),
            Self::Insert(stmt) => stmt.to_string(PostgresQueryBuilder),
            Self::Update(stmt) => stmt.to_string(PostgresQueryBuilder),
            Self::Delete(stmt) => stmt.to_string(PostgresQueryBuilder)
        }
    }

    pub(crate) async fn execute(&self, pool: &Pool<Postgres>) -> Result<(), Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .execute(pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn fetch_id(&self, pool: &Pool<Postgres>) -> Result<i32, Error>
    {
        let (sql, _) = self.build();
        let id = sqlx::query(&sql)
            .map(|row: PgRow| row.get(0))
            .fetch_one(pool)
            .await?;
        Ok(id)
    }

    pub(crate) async fn fetch_max_order(&self, pool: &Pool<Postgres>, default: i32) -> i32
    {
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| row.try_get(0))
            .fetch_one(pool)
            .await
            .unwrap_or(Ok(default))
            .unwrap_or(default)
    }

    pub(crate) async fn fetch_count(&self, pool: &Pool<Postgres>) -> Result<usize, Error>
    {
        let (sql, arguments) = self.build();
        let count: i64 = sqlx::query_with(&sql, arguments)
            .map(|row| {
                row.get(0)
            })
            .fetch_one(pool)
            .await?;
        Ok(count as usize)
    }

    pub(crate) async fn fetch_timestamp(&self, pool: &Pool<Postgres>) -> Result<Vec<DateTime<Utc>>, Error>
    {
        let (sql, arguments) = self.build();
        let mut rows = sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                row.get(0)
            })
            .fetch_all(pool)
            .await?;
        rows.dedup();
        Ok(rows)
    }

}
