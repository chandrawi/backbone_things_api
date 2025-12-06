use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{SelectStatement, InsertStatement, UpdateStatement, DeleteStatement, PostgresQueryBuilder};
use sea_query_binder::{SqlxBinder, SqlxValues};
use crate::auth::_schema::{
    ApiSchema, ProcedureSchema, RoleSchema, UserSchema, RoleProfileSchema, UserProfileSchema, TokenSchema
};
use crate::auth::_row::{
    ApiRow, ProcedureRow, RoleRow, UserRow,
    map_to_api_schema, map_to_procedure_schema, map_to_role_schema, map_to_user_schema
};

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
            .map(|row: PgRow| row.try_get(0))
            .fetch_one(pool)
            .await??;
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
            .map(|row: PgRow| row.try_get(0))
            .fetch_one(pool)
            .await??;
        Ok(count as usize)
    }

    pub(crate) async fn fetch_timestamp(&self, pool: &Pool<Postgres>) -> Result<Vec<DateTime<Utc>>, Error>
    {
        let (sql, arguments) = self.build();
        let results = sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| row.try_get::<DateTime<Utc>,_>(0))
            .fetch_all(pool)
            .await?;
        let mut rows = Vec::new();
        for result in results {
            rows.push(result?);
        }
        rows.dedup();
        Ok(rows)
    }

    pub(crate) async fn fetch_api_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<ApiSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<ApiRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_api_schema(rows))
    }

    pub(crate) async fn fetch_procedure_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<ProcedureSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<ProcedureRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_procedure_schema(rows))
    }

    pub(crate) async fn fetch_role_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<RoleSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<RoleRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_role_schema(rows))
    }

    pub(crate) async fn fetch_user_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<UserSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<UserRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_user_schema(rows))
    }

    pub(crate) async fn fetch_role_profile_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<RoleProfileSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_user_profile_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<UserProfileSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_token_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<TokenSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

}
