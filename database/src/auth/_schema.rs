use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
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
