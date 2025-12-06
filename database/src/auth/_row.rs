use sqlx::{Row, FromRow, Error, postgres::PgRow};
use uuid::Uuid;
use crate::auth::_schema::{
    ApiSchema, ProcedureSchema, RoleSchema, UserSchema, UserRoleSchema, 
    ProfileMode, RoleProfileSchema, UserProfileSchema, TokenSchema
};
use crate::common::type_value::{DataType, DataValue};

pub(crate) struct ApiRow {
    api_id: Uuid,
    name: String,
    address: String,
    category: String,
    description: String,
    password: String,
    access_key: Vec<u8>,
    procedure_id: Option<Uuid>,
    procedure_name: Option<String>,
    procedure_description: Option<String>,
    role_name: Option<String>
}

impl<'r> FromRow<'r, PgRow> for ApiRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            api_id: row.try_get(0)?,
            name: row.try_get(1)?,
            address: row.try_get(2)?,
            category: row.try_get(3)?,
            description: row.try_get(4)?,
            password: row.try_get(5)?,
            access_key: row.try_get(6)?,
            procedure_id: row.try_get(7)?,
            procedure_name: row.try_get(8)?,
            procedure_description: row.try_get(9)?,
            role_name: row.try_get(10)?
        })
    }
}

pub(crate) fn map_to_api_schema(rows: Vec<ApiRow>) -> Vec<ApiSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // ApiRow is sorted by (api_id, procedure_id) from query result

    let mut result = Vec::new();
    let mut last_api_id: Option<Uuid> = None;
    let mut last_proc_id: Option<Uuid> = None;
    let mut last_api: Option<ApiSchema> = None;
    let mut last_proc: Option<ProcedureSchema> = None;

    for row in rows {
        // 1) Detect new API row
        if Some(row.api_id) != last_api_id {
            // Push previous api into result
            if let Some(mut api) = last_api.take() {
                if let Some(proc) = last_proc.take() {
                    api.procedures.push(proc);
                }
                result.push(api);
            }
            // start new api
            last_api_id = Some(row.api_id);
            last_api = Some(ApiSchema {
                id: row.api_id,
                name: row.name,
                address: row.address,
                category: row.category,
                description: row.description,
                password: row.password,
                access_key: row.access_key,
                procedures: Vec::new(),
            });
            // reset proc state
            last_proc_id = None;
            last_proc = None;
        }

        // 2) Detect new procedure row
        if let Some(proc_id) = row.procedure_id {
            if Some(proc_id) != last_proc_id {
                // push previous procedure into last_api
                if let (Some(api), Some(proc)) = (last_api.as_mut(), last_proc.take()) {
                    api.procedures.push(proc);
                }
                last_proc_id = Some(proc_id);
                last_proc = Some(ProcedureSchema {
                    id: proc_id,
                    api_id: row.api_id,
                    name: row.procedure_name.unwrap_or_default(),
                    description: row.procedure_description.unwrap_or_default(),
                    roles: Vec::new(),
                });
            }

            // 3) Add role if exists
            if let (Some(proc), Some(role)) = (last_proc.as_mut(), row.role_name) {
                proc.roles.push(role);
            }
        }
    }

    // Push last procedure and last api
    if let Some(mut api) = last_api {
        if let Some(proc) = last_proc {
            api.procedures.push(proc);
        }
        result.push(api);
    }
    result
}

pub(crate) struct ProcedureRow {
    procedure_id: Uuid,
    api_id: Uuid,
    name: String,
    description: String,
    role_name: Option<String>
}

impl<'r> FromRow<'r, PgRow> for ProcedureRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            procedure_id: row.try_get(0)?,
            api_id: row.try_get(1)?,
            name: row.try_get(2)?,
            description: row.try_get(3)?,
            role_name: row.try_get(4)?
        })
    }
}

pub(crate) fn map_to_procedure_schema(rows: Vec<ProcedureRow>) -> Vec<ProcedureSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // ProcedureRow is sorted by (procedure_id) from query result

    let mut result = Vec::new();
    let mut last_proc_id: Option<Uuid> = None;
    let mut last_proc: Option<ProcedureSchema> = None;

    for row in rows {
        // 1) Detect new procedure row
        if Some(row.procedure_id) != last_proc_id {
            // Push previous procedure
            if let Some(proc) = last_proc.take() {
                result.push(proc);
            }
            last_proc_id = Some(row.procedure_id);
            last_proc = Some(ProcedureSchema {
                id: row.procedure_id,
                api_id: row.api_id,
                name: row.name,
                description: row.description,
                roles: Vec::new(),
            });
        }

        // 2) Add role if exists
        if let (Some(proc), Some(role)) = (last_proc.as_mut(), row.role_name) {
            proc.roles.push(role);
        }
    }

    // Push last procedure
    if let Some(proc) = last_proc.take() {
        result.push(proc);
    }
    result
}

pub(crate) struct RoleRow {
    role_id: Uuid,
    api_id: Uuid,
    name: String,
    multi: bool,
    ip_lock: bool,
    access_duration: i32,
    refresh_duration: i32,
    access_key: Vec<u8>,
    procedure_id: Option<Uuid>
}

impl<'r> FromRow<'r, PgRow> for RoleRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            role_id: row.try_get(0)?,
            api_id: row.try_get(1)?,
            name: row.try_get(2)?,
            multi: row.try_get(3)?,
            ip_lock: row.try_get(4)?,
            access_duration: row.try_get(5)?,
            refresh_duration: row.try_get(6)?,
            access_key: row.try_get(7)?,
            procedure_id: row.try_get(8)?
        })
    }
}

pub(crate) fn map_to_role_schema(rows: Vec<RoleRow>) -> Vec<RoleSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // RoleRow is sorted by (role_id, procedure_id) from query result

    let mut result = Vec::<RoleSchema>::new();
    let mut last_role_id: Option<Uuid> = None;
    let mut last_role: Option<RoleSchema> = None;

    for row in rows {
        // 1) Detect new role row
        if Some(row.role_id) != last_role_id {
            // Push previous role
            if let Some(role) = last_role.take() {
                result.push(role);
            }
            last_role_id = Some(row.role_id);
            last_role = Some(RoleSchema {
                id: row.role_id,
                api_id: row.api_id,
                name: row.name,
                multi: row.multi,
                ip_lock: row.ip_lock,
                access_duration: row.access_duration,
                refresh_duration: row.refresh_duration,
                access_key: row.access_key,
                procedures: Vec::new(),
            });
        }

        // 2) Add procedure id if exists
        if let (Some(role), Some(proc_id)) = (last_role.as_mut(), row.procedure_id) {
            role.procedures.push(proc_id);
        }
    }

    // Push last role
    if let Some(role) = last_role.take() {
        result.push(role);
    }
    result
}

pub(crate) struct UserRow {
    user_id: Uuid,
    name: String,
    password: String,
    email: String,
    phone: String,
    api_id: Option<Uuid>,
    role_name: Option<String>,
    multi: Option<bool>,
    ip_lock: Option<bool>,
    access_duration: Option<i32>,
    refresh_duration: Option<i32>,
    access_key: Option<Vec<u8>>
}

impl<'r> FromRow<'r, PgRow> for UserRow {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            user_id: row.try_get(0)?,
            name: row.try_get(1)?,
            password: row.try_get(2)?,
            email: row.try_get(3)?,
            phone: row.try_get(4)?,
            api_id: row.try_get(5)?,
            role_name: row.try_get(6)?,
            multi: row.try_get(7)?,
            ip_lock: row.try_get(8)?,
            access_duration: row.try_get(9)?,
            refresh_duration: row.try_get(10)?,
            access_key: row.try_get(11)?
        })
    }
}

pub(crate) fn map_to_user_schema(rows: Vec<UserRow>) -> Vec<UserSchema> {
    if rows.is_empty() {
        return Vec::new();
    }
    // UserRow is sorted by (user_id, role_id) from query result

    let mut result = Vec::<UserSchema>::new();
    let mut last_user_id: Option<Uuid> = None;
    let mut last_user: Option<UserSchema> = None;

    for row in rows {
        // 1) Detect new user row
        if Some(row.user_id) != last_user_id {
            // Push previous user
            if let Some(user) = last_user.take() {
                result.push(user);
            }
            last_user_id = Some(row.user_id);
            last_user = Some(UserSchema {
                id: row.user_id,
                name: row.name,
                email: row.email,
                phone: row.phone,
                password: row.password,
                roles: Vec::new(),
            });
        }

        // 2) Add user role if exists
        if let (Some(user), Some(api_id), Some(role_name)) = (last_user.as_mut(), row.api_id, row.role_name) {
            user.roles.push(UserRoleSchema {
                api_id: api_id,
                role: role_name,
                multi: row.multi.unwrap_or_default(),
                ip_lock: row.ip_lock.unwrap_or_default(),
                access_duration: row.access_duration.unwrap_or_default(),
                refresh_duration: row.refresh_duration.unwrap_or_default(),
                access_key: row.access_key.unwrap_or_default()
            });
        }
    }

    // Push last user
    if let Some(user) = last_user.take() {
        result.push(user);
    }
    result
}

impl<'r> FromRow<'r, PgRow> for RoleProfileSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let type_number: i16 = row.try_get(3)?;
        let mode_number: i16 = row.try_get(4)?;
        Ok(Self {
            id: row.try_get(0)?,
            role_id: row.try_get(1)?,
            name: row.try_get(2)?,
            value_type: DataType::from(type_number),
            mode: ProfileMode::from(mode_number)
        })
    }
}

impl<'r> FromRow<'r, PgRow> for UserProfileSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        let bytes: Vec<u8> = row.try_get(4)?;
        let type_number: i16 = row.try_get(5)?;
        Ok(Self {
            id: row.try_get(0)?,
            user_id: row.try_get(1)?,
            name: row.try_get(2)?,
            value: DataValue::from_bytes(&bytes, DataType::from(type_number)),
            order: row.try_get(3)?
        })
    }
}

impl<'r> FromRow<'r, PgRow> for TokenSchema {
    fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self {
            access_id: row.try_get(0)?,
            user_id: row.try_get(1)?,
            refresh_token: row.try_get(2)?,
            auth_token: row.try_get(3)?,
            created: row.try_get(4)?,
            expired: row.try_get(5)?,
            ip: row.try_get(6)?
        })
    }
}
