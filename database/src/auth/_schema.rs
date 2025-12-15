use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
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
    pub procedure_ids: Vec<Uuid>
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
pub struct RoleProfileSchema {
    pub id: i32,
    pub role_id: Uuid,
    pub name: String,
    pub value_type: DataType,
    pub value_default: DataValue,
    pub category: String
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct UserProfileSchema {
    pub id: i32,
    pub user_id: Uuid,
    pub name: String,
    pub value: DataValue,
    pub category: String
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
