use chrono::{Utc, TimeZone};
use uuid::Uuid;
use bbthings_database::{DataType, DataValue};
use bbthings_database::{
    ApiSchema, ProcedureSchema, RoleSchema, UserSchema, UserRoleSchema,
    RoleProfileSchema, UserProfileSchema, TokenSchema
};
use crate::proto::auth::{
    api, role, user, profile, token
};

impl From<ApiSchema> for api::ApiSchema {
    fn from(value: ApiSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            name: value.name,
            address: value.address,
            category: value.category,
            description: value.description,
            password: value.password.as_bytes().to_vec(),
            access_key: value.access_key,
            procedures: value.procedures.into_iter().map(|v| v.into()).collect()
        }
    }
}

impl From<api::ApiSchema> for ApiSchema {
    fn from(value: api::ApiSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            address: value.address,
            category: value.category,
            description: value.description,
            password: String::from_utf8(value.password).unwrap_or_default(),
            access_key: value.access_key,
            procedures: value.procedures.into_iter().map(|v| v.into()).collect()
        }
    }
}

impl From<ProcedureSchema> for api::ProcedureSchema {
    fn from(value: ProcedureSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            api_id: value.api_id.as_bytes().to_vec(),
            name: value.name,
            description: value.description,
            roles: value.roles
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
            roles: value.roles
        }
    }
}

impl From<RoleSchema> for role::RoleSchema {
    fn from(value: RoleSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            api_id: value.api_id.as_bytes().to_vec(),
            name: value.name,
            multi: value.multi,
            ip_lock: value.ip_lock,
            access_duration: value.access_duration,
            refresh_duration: value.refresh_duration,
            access_key: value.access_key,
            procedure_ids: value.procedure_ids.into_iter().map(|v| v.as_bytes().to_vec()).collect()
        }
    }
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
            procedure_ids: value.procedure_ids.into_iter().map(|v| Uuid::from_slice(&v).unwrap_or_default()).collect()
        }
    }
}

impl From<UserSchema> for user::UserSchema {
    fn from(value: UserSchema) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            name: value.name,
            email: value.email,
            phone: value.phone,
            password: value.password.as_bytes().to_vec(),
            roles: value.roles.into_iter().map(|v| v.into()).collect()
        }
    }
}

impl From<user::UserSchema> for UserSchema {
    fn from(value: user::UserSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            email: value.email,
            phone: value.phone,
            password: String::from_utf8(value.password).unwrap_or_default(),
            roles: value.roles.into_iter().map(|v| v.into()).collect()
        }
    }
}

impl From<UserRoleSchema> for user::UserRoleSchema {
    fn from(value: UserRoleSchema) -> Self {
        Self {
            api_id: value.api_id.as_bytes().to_vec(),
            role: value.role,
            multi: value.multi,
            ip_lock: value.ip_lock,
            access_duration: value.access_duration,
            refresh_duration: value.refresh_duration,
            access_key: value.access_key
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

impl From<RoleProfileSchema> for profile::RoleProfileSchema {
    fn from(value: RoleProfileSchema) -> Self {
        Self {
            id: value.id,
            role_id: value.role_id.as_bytes().to_vec(),
            name: value.name,
            value_type: value.value_type.into(),
            value_bytes: value.value_default.to_bytes(),
            category: value.category
        }
    }
}

impl From<profile::RoleProfileSchema> for RoleProfileSchema {
    fn from(value: profile::RoleProfileSchema) -> Self {
        Self {
            id: value.id,
            role_id: Uuid::from_slice(&value.role_id).unwrap_or_default(),
            name: value.name,
            value_type: value.value_type.into(),
            value_default: DataValue::from_bytes(value.value_bytes.as_slice(), DataType::from(value.value_type)),
            category: value.category
        }
    }
}

impl From<UserProfileSchema> for profile::UserProfileSchema {
    fn from(value: UserProfileSchema) -> Self {
        Self {
            id: value.id,
            user_id: value.user_id.as_bytes().to_vec(),
            name: value.name,
            value_bytes: value.value.to_bytes(),
            value_type: value.value.get_type().into(),
            category: value.category
        }
    }
}

impl From<profile::UserProfileSchema> for UserProfileSchema {
    fn from(value: profile::UserProfileSchema) -> Self {
        Self {
            id: value.id,
            user_id: Uuid::from_slice(&value.user_id).unwrap_or_default(),
            name: value.name,
            value: DataValue::from_bytes(value.value_bytes.as_slice(), DataType::from(value.value_type)),
            category: value.category
        }
    }
}

impl From<TokenSchema> for token::TokenSchema {
    fn from(value: TokenSchema) -> Self {
        Self {
            access_id: value.access_id,
            user_id: value.user_id.as_bytes().to_vec(),
            refresh_token: value.refresh_token,
            auth_token: value.auth_token,
            created: value.created.timestamp_micros(),
            expired: value.expired.timestamp_micros(),
            ip: value.ip
        }
    }
}

impl From<token::TokenSchema> for TokenSchema {
    fn from(value: token::TokenSchema) -> Self {
        Self {
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
