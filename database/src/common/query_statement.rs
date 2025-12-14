use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use sea_query::{SelectStatement, InsertStatement, UpdateStatement, DeleteStatement, PostgresQueryBuilder};
use sea_query_binder::{SqlxBinder, SqlxValues};
use crate::auth::_schema::{
    ApiSchema, ProcedureSchema, RoleSchema, UserSchema, RoleProfileSchema, UserProfileSchema, TokenSchema
};
use crate::auth::_row::{
    ApiRow, ProcedureRow, RoleRow, UserRow,
    map_to_api_schema, map_to_procedure_schema, map_to_role_schema, map_to_user_schema
};
use crate::resource::_schema::{
    ModelSchema, TagSchema, ModelConfigSchema, DeviceSchema, TypeSchema, DeviceConfigSchema, TypeConfigSchema,
    GroupSchema, SetSchema, SetMember, SetTemplateSchema, SetTemplateMember,
    DataSchema, DataSetSchema, BufferSchema, BufferSetSchema, SliceSchema, SliceSetSchema
};
use crate::resource::_row::{
    ModelRow, DeviceRow, TypeRow, GroupRow, SetRow, SetTemplateRow, DataSetRow, BufferSetRow,
    map_to_model_schema, map_to_device_schema, map_to_type_schema, map_to_group_schema,
    map_to_set_schema, map_to_set_template_schema, map_to_dataset_schema, map_to_bufferset_schema
};
use crate::common::type_value::DataType;

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
        let (sql, arguments) = self.build();
        let id = sqlx::query_with(&sql, arguments)
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

    pub(crate) async fn fetch_model_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<ModelSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<ModelRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_model_schema(rows))
    }

    pub(crate) async fn fetch_model_config_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<ModelConfigSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_tag_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<TagSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_tag_members(&self, pool: &Pool<Postgres>, tag: i16) -> Vec<i16>
    {
        let mut tags: Vec<i16> = vec![tag];
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                let bytes: Vec<u8> = row.try_get(0).unwrap_or_default();
                for chunk in bytes.chunks_exact(2) {
                    tags.push(i16::from_be_bytes([chunk[0], chunk[1]]));
                }
            })
            .fetch_all(pool)
            .await
            .unwrap_or_default();
        tags.sort();
        tags.dedup();
        tags
    }

    pub(crate) async fn fetch_device_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<DeviceSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<DeviceRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_device_schema(rows))
    }

    pub(crate) async fn fetch_device_config_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<DeviceConfigSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_type_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<TypeSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<TypeRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_type_schema(rows))
    }

    pub(crate) async fn fetch_type_config_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<TypeConfigSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_group_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<GroupSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<GroupRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_group_schema(rows))
    }

    pub(crate) async fn fetch_set_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<SetSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<SetRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_set_schema(rows))
    }

    pub(crate) async fn fetch_set_members(&self, pool: &Pool<Postgres>) -> Result<Vec<SetMember>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_set_template_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<SetTemplateSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<SetTemplateRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_set_template_schema(rows))
    }

    pub(crate) async fn fetch_set_template_members(&self, pool: &Pool<Postgres>) -> Result<Vec<SetTemplateMember>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_data_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<DataSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_data_types(&self, pool: &Pool<Postgres>) -> Result<Vec<Vec<DataType>>, Error>
    {
        let (sql, arguments) = self.build();
        let results = sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| row.try_get::<Vec<u8>,_>(0))
            .fetch_all(pool)
            .await?;
        let mut types_vec = Vec::new();
        for result in results {
            let types: Vec<DataType> = result?.into_iter().map(|t| t.into()).collect();
            types_vec.push(types);
        }
        Ok(types_vec)
    }

    pub(crate) async fn fetch_data_set_schema(&self, pool: &Pool<Postgres>, set_id: Uuid) -> Result<Vec<DataSetSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<DataSetRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_dataset_schema(rows, set_id))
    }

    pub(crate) async fn fetch_buffer_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<BufferSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_buffer_types(&self, pool: &Pool<Postgres>) -> Result<Vec<DataType>, Error>
    {
        let (sql, arguments) = self.build();
        let result = sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| row.try_get::<Vec<u8>,_>(0))
            .fetch_one(pool)
            .await?;
        Ok(result?.into_iter().map(|t| t.into()).collect())
    }

    pub(crate) async fn fetch_buffer_set_schema(&self, pool: &Pool<Postgres>, set_id: Uuid) -> Result<Vec<BufferSetSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows: Vec<BufferSetRow> = sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await?;
        Ok(map_to_bufferset_schema(rows, set_id))
    }

    pub(crate) async fn fetch_slice_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<SliceSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

    pub(crate) async fn fetch_slice_set_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<SliceSetSchema>, Error>
    {
        let (sql, arguments) = self.build();
        sqlx::query_as_with(&sql, arguments)
            .fetch_all(pool)
            .await
    }

}
