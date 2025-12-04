use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
use crate::common::query_statement::QueryStatement;
use crate::common::type_value::{DataType, DataValue};
use bbthings_grpc_proto::auth::{api, role, user, profile, token};

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

impl From<api::ApiSchema> for ApiSchema {
    fn from(value: api::ApiSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            address: value.address,
            category: value.category,
            description: value.description,
            password: value.password,
            access_key: value.access_key,
            procedures: value.procedures.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<api::ApiSchema> for ApiSchema {
    fn into(self) -> api::ApiSchema {
        api::ApiSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            address: self.address,
            category: self.category,
            description: self.description,
            password: self.password,
            access_key: self.access_key,
            procedures: self.procedures.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<api::ProcedureSchema> for ProcedureSchema {
    fn from(value: api::ProcedureSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            api_id: Uuid::from_slice(&value.api_id).unwrap_or_default(),
            name: value.name,
            description: value.description,
            roles: value.roles.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<api::ProcedureSchema> for ProcedureSchema {
    fn into(self) -> api::ProcedureSchema {
        api::ProcedureSchema {
            id: self.id.as_bytes().to_vec(),
            api_id: self.api_id.as_bytes().to_vec(),
            name: self.name,
            description: self.description,
            roles: self.roles.into_iter().map(|e| e.into()).collect()
        }
    }
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

impl From<role::RoleSchema> for RoleSchema {
    fn from(value: role::RoleSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            api_id: Uuid::from_slice(&value.api_id).unwrap_or_default(),
            name: value.name,
            multi: value.multi,
            ip_lock: value.ip_lock,
            access_duration: value.access_duration,
            refresh_duration: value.refresh_duration,
            access_key: value.access_key,
            procedures: value.procedures.into_iter().map(|u| Uuid::from_slice(&u).unwrap_or_default()).collect()
        }
    }
}

impl Into<role::RoleSchema> for RoleSchema {
    fn into(self) -> role::RoleSchema {
        role::RoleSchema {
            id: self.id.as_bytes().to_vec(),
            api_id: self.api_id.as_bytes().to_vec(),
            name: self.name,
            multi: self.multi,
            ip_lock: self.ip_lock,
            access_duration: self.access_duration,
            refresh_duration: self.refresh_duration,
            access_key: self.access_key,
            procedures: self.procedures.into_iter().map(|u| u.as_bytes().to_vec()).collect()
        }
    }
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

impl From<user::UserSchema> for UserSchema {
    fn from(value: user::UserSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            email: value.email,
            phone: value.phone,
            password: value.password,
            roles: value.roles.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<user::UserSchema> for UserSchema {
    fn into(self) -> user::UserSchema {
        user::UserSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            email: self.email,
            phone: self.phone,
            password: self.password,
            roles: self.roles.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<user::UserRoleSchema> for UserRoleSchema {
    fn from(value: user::UserRoleSchema) -> Self {
        Self {
            api_id: Uuid::from_slice(&value.api_id).unwrap_or_default(),
            role: value.role,
            multi: value.multi,
            ip_lock: value.ip_lock,
            access_duration: value.access_duration,
            refresh_duration: value.refresh_duration,
            access_key: value.access_key
        }
    }
}

impl Into<user::UserRoleSchema> for UserRoleSchema {
    fn into(self) -> user::UserRoleSchema {
        user::UserRoleSchema {
            api_id: self.api_id.as_bytes().to_vec(),
            role: self.role,
            multi: self.multi,
            ip_lock: self.ip_lock,
            access_duration: self.access_duration,
            refresh_duration: self.refresh_duration,
            access_key: self.access_key
        }
    }
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

impl From<u32> for ProfileMode {
    fn from(value: u32) -> Self {
        Self::from(value as i16)
    }
}

impl From<ProfileMode> for u32 {
    fn from(value: ProfileMode) -> Self {
        i16::from(value) as u32
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

impl From<profile::RoleProfileSchema> for RoleProfileSchema {
    fn from(value: profile::RoleProfileSchema) -> Self {
        Self {
            id: value.id,
            role_id: Uuid::from_slice(&value.role_id).unwrap_or_default(),
            name: value.name,
            value_type: DataType::from(value.value_type),
            mode: ProfileMode::from(value.mode)
        }
    }
}

impl Into<profile::RoleProfileSchema> for RoleProfileSchema {
    fn into(self) -> profile::RoleProfileSchema {
        profile::RoleProfileSchema {
            id: self.id,
            role_id: self.role_id.as_bytes().to_vec(),
            name: self.name,
            value_type: u32::from(self.value_type),
            mode: self.mode.into()
        }
    }
}

impl From<profile::UserProfileSchema> for UserProfileSchema {
    fn from(value: profile::UserProfileSchema) -> Self {
        Self {
            id: value.id,
            user_id: Uuid::from_slice(&value.user_id).unwrap_or_default(),
            name: value.name,
            order: value.order as i16,
            value: DataValue::from_bytes(value.value_bytes.as_slice(), DataType::from(value.value_type))
        }
    }
}

impl Into<profile::UserProfileSchema> for UserProfileSchema {
    fn into(self) -> profile::UserProfileSchema {
        profile::UserProfileSchema {
            id: self.id,
            user_id: self.user_id.as_bytes().to_vec(),
            name: self.name,
            order: self.order as u32,
            value_type: u32::from(self.value.get_type()),
            value_bytes: self.value.to_bytes()
        }
    }
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

impl From<token::TokenSchema> for TokenSchema {
    fn from(value: token::TokenSchema) -> Self {
        TokenSchema {
            access_id: value.access_id,
            user_id: Uuid::from_slice(&value.user_id).unwrap_or_default(),
            refresh_token: value.refresh_token,
            auth_token: value.auth_token,
            created: Utc.timestamp_nanos(value.created * 1000),
            expired: Utc.timestamp_nanos(value.expired * 1000),
            ip: value.ip
        }
    }
}

impl Into<token::TokenSchema> for TokenSchema {
    fn into(self) -> token::TokenSchema {
        token::TokenSchema {
            access_id: self.access_id,
            user_id: self.user_id.as_bytes().to_vec(),
            refresh_token: self.refresh_token,
            auth_token: self.auth_token,
            created: self.created.timestamp_micros(),
            expired: self.expired.timestamp_micros(),
            ip: self.ip
        }
    }
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
