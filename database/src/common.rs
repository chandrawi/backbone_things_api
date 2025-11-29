use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sea_query_binder::SqlxValues;

#[derive(Debug, Clone)]
pub(crate) struct QuerySet {
    pub(crate) query: String,
    pub(crate) values: SqlxValues
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

}
