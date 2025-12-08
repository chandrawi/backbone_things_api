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
