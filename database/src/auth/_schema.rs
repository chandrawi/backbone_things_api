use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;
use crate::common::type_value::{DataType, DataValue};

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ApiSchema {
    pub id: Uuid,
    pub name: String,
    pub address: String,
    pub category: String,
    pub description: String,
    pub password: String,
    pub access_key: Vec<u8>,
    pub procedures: Vec<ProcedureSchema>,
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ProcedureSchema {
    pub id: Uuid,
    pub api_id: Uuid,
    pub name: String,
    pub description: String,
    pub roles: Vec<String>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct RoleSchema {
    pub id: Uuid,
    pub api_id: Uuid,
    pub name: String,
    pub multi: bool,
    pub ip_lock: bool,
    pub access_duration: i32,
    pub refresh_duration: i32,
    pub access_key: Vec<u8>,
    pub procedures: Vec<Uuid>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct UserSchema {
    pub id: Uuid,
    pub name: String,
    pub email: String,
    pub phone: String,
    pub password: String,
    pub roles: Vec<UserRoleSchema>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct UserRoleSchema {
    pub api_id: Uuid,
    pub role: String,
    pub multi: bool,
    pub ip_lock: bool,
    pub access_duration: i32,
    pub refresh_duration: i32,
    pub access_key: Vec<u8>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub enum ProfileMode {
    #[default]
    SingleOptional,
    SingleRequired,
    MultipleOptional,
    MultipleRequired
}

impl From<i16> for ProfileMode {
    fn from(value: i16) -> Self {
        match value {
            1 => Self::SingleRequired,
            2 => Self::MultipleOptional,
            3 => Self::MultipleRequired,
            _ => Self::SingleOptional
        }
    }
}

impl From<ProfileMode> for i16 {
    fn from(value: ProfileMode) -> Self {
        match &value {
            ProfileMode::SingleOptional => 0,
            ProfileMode::SingleRequired => 1,
            ProfileMode::MultipleOptional => 2,
            ProfileMode::MultipleRequired => 3
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct RoleProfileSchema {
    pub id: i32,
    pub role_id: Uuid,
    pub name: String,
    pub value_type: DataType,
    pub mode: ProfileMode
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct UserProfileSchema {
    pub id: i32,
    pub user_id: Uuid,
    pub name: String,
    pub value: DataValue,
    pub order: i16
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct TokenSchema {
    pub access_id: i32,
    pub user_id: Uuid,
    pub refresh_token: String,
    pub auth_token: String,
    pub created: DateTime<Utc>,
    pub expired: DateTime<Utc>,
    pub ip: Vec<u8>
}

impl QueryStatement {

    pub(crate) async fn fetch_api_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<ApiSchema>, Error>
    {
        let mut last_id: Option<Uuid> = None;
        let mut last_procedure: Option<Uuid> = None;
        let mut role_vec: Vec<String> = Vec::new();
        let mut api_schema_vec: Vec<ApiSchema> = Vec::new();

        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // get last api_schema in api_schema_vec or default
                let mut api_schema = api_schema_vec.pop().unwrap_or_default();
                // on every new api_id found update last_id and insert new api_schema to api_schema_vec
                let api_id: Uuid = row.get(0);
                if let Some(value) = last_id {
                    if value != api_id {
                        api_schema_vec.push(api_schema.clone());
                        api_schema = ApiSchema::default();
                        last_procedure = None;
                        role_vec = Vec::new();
                    }
                }
                last_id = Some(api_id);
                api_schema.id = api_id;
                api_schema.name = row.get(1);
                api_schema.address = row.get(2);
                api_schema.category = row.get(3);
                api_schema.description = row.get(4);
                api_schema.password = row.get(5);
                api_schema.access_key = row.get(6);
                // on every new procedure_id found add a procedure to api_schema
                let procedure_id = row.try_get(7).ok();
                let procedure_name: String = row.try_get(8).unwrap_or_default();
                if last_procedure == None || last_procedure != procedure_id {
                    if let Some(id) = procedure_id {
                        api_schema.procedures.push(ProcedureSchema {
                            id,
                            api_id,
                            name: procedure_name.clone(),
                            description: row.get(9),
                            roles: Vec::new()
                        });
                    }
                }
                last_procedure = procedure_id;
                // add role to api_schema procedures
                let role_name: Result<String, _> = row.try_get(10);
                if let Ok(name) = role_name {
                    let mut procedure_schema = api_schema.procedures.pop().unwrap_or_default();
                    procedure_schema.roles.push(name.clone());
                    api_schema.procedures.push(procedure_schema);
                    role_vec.push(name);
                }
                // update api_schema_vec with updated api_schema
                api_schema_vec.push(api_schema);
            })
            .fetch_all(pool)
            .await?;

        Ok(api_schema_vec)
    }

    pub(crate) async fn fetch_procedure_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<ProcedureSchema>, Error>
    {
        let mut last_id: Option<Uuid> = None;
        let mut proc_schema_vec: Vec<ProcedureSchema> = Vec::new();

        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // get last proc_schema in proc_schema_vec or default
                let mut proc_schema = proc_schema_vec.pop().unwrap_or_default();
                // on every new proc_id found update last_id and insert new proc_schema to proc_schema_vec
                let proc_id: Uuid = row.get(0);
                if let Some(value) = last_id {
                    if value != proc_id {
                        proc_schema_vec.push(proc_schema.clone());
                        proc_schema = ProcedureSchema::default();
                    }
                }
                last_id = Some(proc_id);
                proc_schema.id = proc_id;
                proc_schema.api_id = row.get(1);
                proc_schema.name = row.get(2);
                proc_schema.description = row.get(3);
                // add role to proc_schema roles
                let role_name: Result<String, _> = row.try_get(4);
                if let Ok(name) = role_name {
                    proc_schema.roles.push(name);
                }
                // update proc_schema_vec with updated proc_schema
                proc_schema_vec.push(proc_schema);
            })
            .fetch_all(pool)
            .await?;

        Ok(proc_schema_vec)
    }

    pub(crate) async fn fetch_role_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<RoleSchema>, Error>
    {
        let mut last_id: Option<Uuid> = None;
        let mut last_procedure: Option<Uuid> = None;
        let mut role_schema_vec: Vec<RoleSchema> = Vec::new();

        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // get last role_schema in role_schema_vec or default
                let mut role_schema = role_schema_vec.pop().unwrap_or_default();
                // on every new role_id found update last_id and insert new role_schema to role_schema_vec
                let role_id: Uuid = row.get(0);
                if let Some(value) = last_id {
                    if value != role_id {
                        role_schema_vec.push(role_schema.clone());
                        role_schema = RoleSchema::default();
                        last_procedure = None;
                    }
                }
                last_id = Some(role_id);
                role_schema.id = role_id;
                role_schema.api_id = row.get(1);
                role_schema.name = row.get(2);
                role_schema.multi = row.get(3);
                role_schema.ip_lock = row.get(4);
                role_schema.access_duration = row.get(5);
                role_schema.refresh_duration = row.get(6);
                role_schema.access_key = row.get(7);
                // on every new procedure_id found add a procedure to role_schema
                let procedure_id = row.try_get(8).ok();
                if last_procedure == None || last_procedure != procedure_id {
                    if let Some(id) = procedure_id {
                        role_schema.procedures.push(id);
                    }
                }
                last_procedure = procedure_id;
                // update role_schema_vec with updated role_schema
                role_schema_vec.push(role_schema);
            })
            .fetch_all(pool)
            .await?;

        Ok(role_schema_vec)
    }

    pub(crate) async fn fetch_user_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<UserSchema>, Error>
    {
        let mut last_id: Option<Uuid> = None;
        let mut user_schema_vec: Vec<UserSchema> = Vec::new();
    
        let (sql, arguments) = self.build();
        sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                // get last user_schema in user_schema_vec or default
                let mut user_schema = user_schema_vec.pop().unwrap_or_default();
                // on every new user_id found update last_id and insert new user_schema to user_schema_vec
                let user_id: Uuid = row.get(0);
                if let Some(value) = last_id {
                    if value != user_id {
                        user_schema_vec.push(user_schema.clone());
                        user_schema = UserSchema::default();
                    }
                }
                last_id = Some(user_id);
                user_schema.id = user_id;
                user_schema.name = row.get(1);
                user_schema.password = row.get(2);
                user_schema.email = row.get(3);
                user_schema.phone = row.get(4);
                // on every new role_id found add a role to user_schema
                let role_name = row.try_get(6).ok();
                if let Some(name) = role_name {
                    user_schema.roles.push(UserRoleSchema {
                        api_id: row.get(5),
                        role: name,
                        multi: row.get(7),
                        ip_lock: row.get(8),
                        access_duration: row.get(9),
                        refresh_duration: row.get(10),
                        access_key: row.get(11)
                    });
                }
                // update api_schema_vec with updated user_schema
                user_schema_vec.push(user_schema);
            })
            .fetch_all(pool)
            .await?;
    
        Ok(user_schema_vec)
    }

    pub(crate) async fn fetch_role_profile_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<RoleProfileSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows = sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                RoleProfileSchema {
                    id: row.get(0),
                    role_id: row.get(1),
                    name: row.get(2),
                    value_type: DataType::from(row.get::<i16,_>(3)),
                    mode: ProfileMode::from(row.get::<i16,_>(4))
                }
            })
            .fetch_all(pool)
            .await?;

        Ok(rows)
    }

    pub(crate) async fn fetch_user_profile_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<UserProfileSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let rows = sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                let bytes = row.get(4);
                let type_ = DataType::from(row.get::<i16,_>(5));
                UserProfileSchema {
                    id: row.get(0),
                    user_id: row.get(1),
                    name: row.get(2),
                    value: DataValue::from_bytes(bytes, type_),
                    order: row.get(3)
                }
            })
            .fetch_all(pool)
            .await?;

        Ok(rows)
    }

    pub(crate) async fn fetch_token_schema(&self, pool: &Pool<Postgres>) -> Result<Vec<TokenSchema>, Error>
    {
        let (sql, arguments) = self.build();
        let row = sqlx::query_with(&sql, arguments)
            .map(|row: PgRow| {
                TokenSchema {
                    access_id: row.get(0),
                    user_id: row.get(1),
                    refresh_token: row.get(2),
                    auth_token: row.get(3),
                    created: row.get(4),
                    expired: row.get(5),
                    ip: row.get(6)
                }
            })
            .fetch_all(pool)
            .await?;

        Ok(row)
    }

}
