pub(crate) mod _schema;
pub mod api;
pub mod role;
pub mod user;
pub mod profile;
pub mod token;

use sqlx::{Pool, Error};
use sqlx::postgres::{Postgres, PgPoolOptions};
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use super::value::{DataType, DataValue};
use _schema::{
    ApiSchema, ProcedureSchema, RoleSchema, UserSchema, 
    RoleProfileSchema, UserProfileSchema, ProfileMode, TokenSchema
};
use token::TokenSelector;
use super::common::{verify_hash_format, generate_token_string};

#[derive(Debug, Clone)]
pub struct Auth {
    pub pool: Pool<Postgres>
}

impl Auth {

    pub async fn new(host: &str, username: &str, password: &str, database: &str) -> Auth {
        let url = format!("postgres://{}:{}@{}/{}", username, password, host, database);
        Auth::new_with_url(&url).await
    }

    pub async fn new_with_url(url: &str) -> Auth {
        let pool = PgPoolOptions::new()
            .max_connections(100)
            .connect(url)
            .await
            .expect(&format!("Error connecting to {}", url));
        Auth { pool }
    }

    pub fn new_with_pool(pool: Pool<Postgres>) -> Auth {
        Auth { pool }
    }

    pub async fn read_api(&self, id: Uuid)
        -> Result<ApiSchema, Error>
    {
        let qs = api::select_api(Some(id), None, None, None, None);
        qs.fetch_api_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_api_by_name(&self, name: &str)
        -> Result<ApiSchema, Error>
    {
        let qs = api::select_api(None, None, Some(name), None, None);
        qs.fetch_api_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_api_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<ApiSchema>, Error>
    {
        let qs = api::select_api(None, Some(ids), None, None, None);
        qs.fetch_api_schema(&self.pool).await
    }

    pub async fn list_api_by_name(&self, name: &str)
        -> Result<Vec<ApiSchema>, Error>
    {
        let qs = api::select_api(None, None, None, Some(name), None);
        qs.fetch_api_schema(&self.pool).await
    }

    pub async fn list_api_by_category(&self, category: &str)
        -> Result<Vec<ApiSchema>, Error>
    {
        let qs = api::select_api(None, None, None, None, Some(category));
        qs.fetch_api_schema(&self.pool).await
    }

    pub async fn list_api_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<ApiSchema>, Error>
    {
        let qs = api::select_api(None, None, None, name, category);
        qs.fetch_api_schema(&self.pool).await
    }

    pub async fn create_api(&self, id: Uuid, name: &str, address: &str, category: &str, description: &str, password_hash: &str, access_key: &[u8])
        -> Result<Uuid, Error>
    {
        verify_hash_format(password_hash)?;
        let qs = api::insert_api(id, name, address, category, description, &password_hash, access_key);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_api(&self, id: Uuid, name: Option<&str>, address: Option<&str>, category: Option<&str>, description: Option<&str>, password_hash: Option<&str>, access_key: Option<&[u8]>)
        -> Result<(), Error>
    {
        if let Some(hash) = password_hash {
            verify_hash_format(hash)?;
        }
        let qs = api::update_api(id, name, address, category, description, password_hash, access_key);
        qs.execute(&self.pool).await
    }

    pub async fn delete_api(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = api::delete_api(id);
        qs.execute(&self.pool).await
    }

    pub async fn read_procedure(&self, id: Uuid)
        -> Result<ProcedureSchema, Error>
    {
        let qs = api::select_procedure(Some(id), None, None, None, None);
        qs.fetch_procedure_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_procedure_by_name(&self, api_id: Uuid, name: &str)
        -> Result<ProcedureSchema, Error>
    {
        let qs = api::select_procedure(None, None, Some(api_id), Some(name), None);
        qs.fetch_procedure_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_procedure_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<ProcedureSchema>, Error>
    {
        let qs = api::select_procedure(None, Some(ids), None, None, None);
        qs.fetch_procedure_schema(&self.pool).await
    }

    pub async fn list_procedure_by_api(&self, api_id: Uuid)
        -> Result<Vec<ProcedureSchema>, Error>
    {
        let qs = api::select_procedure(None, None, Some(api_id), None, None);
        qs.fetch_procedure_schema(&self.pool).await
    }

    pub async fn list_procedure_by_name(&self, name: &str)
        -> Result<Vec<ProcedureSchema>, Error>
    {
        let qs = api::select_procedure(None, None, None, None, Some(name));
        qs.fetch_procedure_schema(&self.pool).await
    }

    pub async fn list_procedure_option(&self, api_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<ProcedureSchema>, Error>
    {
        let qs = api::select_procedure(None, None, api_id, None, name);
        qs.fetch_procedure_schema(&self.pool).await
    }

    pub async fn create_procedure(&self, id: Uuid, api_id: Uuid, name: &str, description: &str)
        -> Result<Uuid, Error>
    {
        let qs = api::insert_procedure(id, api_id, name, description);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_procedure(&self, id: Uuid, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        let qs = api::update_procedure(id, name, description);
        qs.execute(&self.pool).await
    }

    pub async fn delete_procedure(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = api::delete_procedure(id);
        qs.execute(&self.pool).await
    }

    pub async fn read_role(&self, id: Uuid)
        -> Result<RoleSchema, Error>
    {
        let qs = role::select_role(Some(id), None, None, None, None, None);
        qs.fetch_role_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_role_by_name(&self, api_id: Uuid, name: &str)
        -> Result<RoleSchema, Error>
    {
        let qs = role::select_role(None, None, Some(api_id), None, Some(name), None);
        qs.fetch_role_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_role_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<RoleSchema>, Error>
    {
        let qs = role::select_role(None, Some(ids), None, None, None, None);
        qs.fetch_role_schema(&self.pool).await
    }

    pub async fn list_role_by_api(&self, api_id: Uuid)
        -> Result<Vec<RoleSchema>, Error>
    {
        let qs = role::select_role(None, None, Some(api_id), None, None, None);
        qs.fetch_role_schema(&self.pool).await
    }

    pub async fn list_role_by_user(&self, user_id: Uuid)
        -> Result<Vec<RoleSchema>, Error>
    {
        let qs = role::select_role(None, None, None, Some(user_id), None, None);
        qs.fetch_role_schema(&self.pool).await
    }

    pub async fn list_role_by_name(&self, name: &str)
        -> Result<Vec<RoleSchema>, Error>
    {
        let qs = role::select_role(None, None, None, None, None, Some(name));
        qs.fetch_role_schema(&self.pool).await
    }

    pub async fn list_role_option(&self, api_id: Option<Uuid>, user_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<RoleSchema>, Error>
    {
        let qs = role::select_role(None, None, api_id, user_id, None, name);
        qs.fetch_role_schema(&self.pool).await
    }

    pub async fn create_role(&self, id: Uuid, api_id: Uuid, name: &str, multi: bool, ip_lock: bool, access_duration: i32, refresh_duration: i32)
        -> Result<Uuid, Error>
    {
        let qs = role::insert_role(id, api_id, name, multi, ip_lock, access_duration, refresh_duration);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_role(&self, id: Uuid, name: Option<&str>, multi: Option<bool>, ip_lock: Option<bool>, access_duration: Option<i32>, refresh_duration: Option<i32>)
        -> Result<(), Error>
    {
        let qs = role::update_role(id, name, multi, ip_lock, access_duration, refresh_duration);
        qs.execute(&self.pool).await
    }

    pub async fn delete_role(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = role::delete_role(id);
        qs.execute(&self.pool).await
    }

    pub async fn add_role_access(&self, id: Uuid, procedure_id: Uuid)
        -> Result<(), Error>
    {
        let qs = role::insert_role_access(id, procedure_id);
        qs.execute(&self.pool).await
    }

    pub async fn remove_role_access(&self, id: Uuid, procedure_id: Uuid)
        -> Result<(), Error>
    {
        let qs = role::delete_role_access(id, procedure_id);
        qs.execute(&self.pool).await
    }

    pub async fn read_role_profile(&self, id: i32)
        -> Result<RoleProfileSchema, Error>
    {
        let qs = profile::select_role_profile(Some(id), None);
        qs.fetch_role_profile_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_role_profile_by_role(&self, role_id: Uuid)
        -> Result<Vec<RoleProfileSchema>, Error>
    {
        let qs = profile::select_role_profile(None, Some(role_id));
        qs.fetch_role_profile_schema(&self.pool).await
    }

    pub async fn create_role_profile(&self, role_id: Uuid, name: &str, value_type: DataType, mode: ProfileMode)
        -> Result<i32, Error>
    {
        let qs = profile::insert_role_profile(role_id, name, value_type, mode);
        qs.execute(&self.pool).await?;
        let qs = profile::select_role_profile_last_id();
        qs.fetch_id(&self.pool).await
    }

    pub async fn update_role_profile(&self, id: i32, name: Option<&str>, value_type: Option<DataType>, mode: Option<ProfileMode>)
        -> Result<(), Error>
    {
        let qs = profile::update_role_profile(id, name, value_type, mode);
        qs.execute(&self.pool).await
    }

    pub async fn delete_role_profile(&self, id: i32)
        -> Result<(), Error>
    {
        let qs = profile::delete_role_profile(id);
        qs.execute(&self.pool).await
    }

    pub async fn read_user(&self, id: Uuid)
        -> Result<UserSchema, Error>
    {
        let qs = user::select_user(Some(id), None, None, None, None, None);
        qs.fetch_user_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_user_by_name(&self, name: &str)
        -> Result<UserSchema, Error>
    {
        let qs = user::select_user(None, None, None, None, Some(name), None);
        qs.fetch_user_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_user_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<UserSchema>, Error>
    {
        let qs = user::select_user(None, Some(ids), None, None, None, None);
        qs.fetch_user_schema(&self.pool).await
    }

    pub async fn list_user_by_api(&self, api_id: Uuid)
        -> Result<Vec<UserSchema>, Error>
    {
        let qs = user::select_user(None, None, Some(api_id), None, None, None);
        qs.fetch_user_schema(&self.pool).await
    }

    pub async fn list_user_by_role(&self, role_id: Uuid)
        -> Result<Vec<UserSchema>, Error>
    {
        let qs = user::select_user(None, None, None, Some(role_id), None, None);
        qs.fetch_user_schema(&self.pool).await
    }

    pub async fn list_user_by_name(&self, name: &str)
        -> Result<Vec<UserSchema>, Error>
    {
        let qs = user::select_user(None, None, None, None, None, Some(name));
        qs.fetch_user_schema(&self.pool).await
    }

    pub async fn list_user_option(&self, api_id: Option<Uuid>, role_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<UserSchema>, Error>
    {
        let qs = user::select_user(None, None, api_id, role_id, None, name);
        qs.fetch_user_schema(&self.pool).await
    }

    pub async fn create_user(&self, id: Uuid, name: &str, email: &str, phone: &str, password_hash: &str)
        -> Result<Uuid, Error>
    {
        verify_hash_format(password_hash)?;
        let qs = user::insert_user(id, name, email, phone, &password_hash);
        qs.execute(&self.pool).await?;
        Ok(id)
    }

    pub async fn update_user(&self, id: Uuid, name: Option<&str>, email: Option<&str>, phone: Option<&str>, password_hash: Option<&str>)
        -> Result<(), Error>
    {
        if let Some(hash) = password_hash {
            verify_hash_format(hash)?;
        }
        let qs = user::update_user(id, name, email, phone, password_hash);
        qs.execute(&self.pool).await
    }

    pub async fn delete_user(&self, id: Uuid)
        -> Result<(), Error>
    {
        let qs = user::delete_user(id);
        qs.execute(&self.pool).await
    }

    pub async fn add_user_role(&self, id: Uuid, role_id: Uuid)
        -> Result<(), Error>
    {
        let qs = user::insert_user_role(id, role_id);
        qs.execute(&self.pool).await
    }

    pub async fn remove_user_role(&self, id: Uuid, role_id: Uuid)
        -> Result<(), Error>
    {
        let qs = user::delete_user_role(id, role_id);
        qs.execute(&self.pool).await
    }

    pub async fn read_user_profile(&self, id: i32)
        -> Result<UserProfileSchema, Error>
    {
        let qs = profile::select_user_profile(Some(id), None);
        qs.fetch_user_profile_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_user_profile_by_user(&self, user_id: Uuid)
        -> Result<Vec<UserProfileSchema>, Error>
    {
        let qs = profile::select_user_profile(None, Some(user_id));
        qs.fetch_user_profile_schema(&self.pool).await
    }

    pub async fn create_user_profile(&self, user_id: Uuid, name: &str, value: DataValue)
        -> Result<i32, Error>
    {
        let qs = profile::select_user_profile_max_order(user_id, name);
        let new_order = qs.fetch_max_order(&self.pool, -1).await + 1;
        let qs = profile::insert_user_profile(user_id, name, value, new_order as i16);
        qs.execute(&self.pool).await?;
        let qs = profile::select_user_profile_last_id();
        qs.fetch_id(&self.pool).await
    }

    pub async fn update_user_profile(&self, id: i32, name: Option<&str>, value: Option<DataValue>)
        -> Result<(), Error>
    {
        let qs = profile::update_user_profile(id, name, value);
        qs.execute(&self.pool).await
    }

    pub async fn delete_user_profile(&self, id: i32)
        -> Result<(), Error>
    {
        let qs = profile::delete_user_profile(id);
        qs.execute(&self.pool).await
    }

    pub async fn swap_user_profile(&self, user_id: Uuid, name: &str, order_1: i16, order_2: i16)
        -> Result<(), Error>
    {
        let qs = profile::update_user_profile_order(user_id, name, order_1, i16::MAX);
        qs.execute(&self.pool).await?;
        let qs = profile::update_user_profile_order(user_id, name, order_2, order_1);
        qs.execute(&self.pool).await?;
        let qs = profile::update_user_profile_order(user_id, name, i16::MAX, order_2);
        qs.execute(&self.pool).await
    }

    pub async fn read_access_token(&self, access_id: i32)
        -> Result<TokenSchema, Error>
    {
        let qs = token::select_token(TokenSelector::Access(access_id));
        qs.fetch_token_schema(&self.pool).await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_auth_token(&self, auth_token: &str)
        -> Result<Vec<TokenSchema>, Error>
    {
        let qs = token::select_token(TokenSelector::Auth(String::from(auth_token)));
        qs.fetch_token_schema(&self.pool).await
    }

    pub async fn list_token_by_user(&self, user_id: Uuid)
        -> Result<Vec<TokenSchema>, Error>
    {
        let qs = token::select_token(TokenSelector::User(user_id));
        qs.fetch_token_schema(&self.pool).await
    }

    pub async fn list_token_by_created_earlier(&self, earlier: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Error>
    {
        let qs = token::select_token(TokenSelector::CreatedEarlier(earlier, user_id));
        qs.fetch_token_schema(&self.pool).await
    }

    pub async fn list_token_by_created_later(&self, later: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Error>
    {
        let qs = token::select_token(TokenSelector::CreatedLater(later, user_id));
        qs.fetch_token_schema(&self.pool).await
    }

    pub async fn list_token_by_created_range(&self, begin: DateTime<Utc>, end: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Error>
    {
        let qs = token::select_token(TokenSelector::CreatedRange(begin, end, user_id));
        qs.fetch_token_schema(&self.pool).await
    }

    pub async fn list_token_by_expired_earlier(&self, earlier: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Error>
    {
        let qs = token::select_token(TokenSelector::ExpiredEarlier(earlier, user_id));
        qs.fetch_token_schema(&self.pool).await
    }

    pub async fn list_token_by_expired_later(&self, later: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Error>
    {
        let qs = token::select_token(TokenSelector::ExpiredLater(later, user_id));
        qs.fetch_token_schema(&self.pool).await
    }

    pub async fn list_token_by_expired_range(&self, begin: DateTime<Utc>, end: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Error>
    {
        let qs = token::select_token(TokenSelector::ExpiredRange(begin, end, user_id));
        qs.fetch_token_schema(&self.pool).await
    }

    pub async fn list_token_by_range(&self, b_created: DateTime<Utc>, e_created: DateTime<Utc>, b_expired: DateTime<Utc>, e_expired: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Error>
    {
        let qs = token::select_token(TokenSelector::Range(b_created, e_created, b_expired, e_expired, user_id));
        qs.fetch_token_schema(&self.pool).await
    }

    pub async fn create_access_token(&self, user_id: Uuid, auth_token: &str, expired: DateTime<Utc>, ip: &[u8])
        -> Result<(i32, String, String), Error>
    {
        let qs = token::select_token_last_access_id();
        let access_id = qs.fetch_max_order(&self.pool, 0).await + 1;
        let refresh_token = generate_token_string();
        let qs = token::insert_token(user_id, vec![access_id], vec![&refresh_token], vec![auth_token], expired, ip);
        qs.execute(&self.pool).await?;
        Ok((access_id, refresh_token, String::from(auth_token)))
    }

    pub async fn create_auth_token(&self, user_id: Uuid, expired: DateTime<Utc>, ip: &[u8], number: usize)
        -> Result<Vec<(i32, String, String)>, Error>
    {
        let qs = token::select_token_last_access_id();
        let access_id = qs.fetch_max_order(&self.pool, 0).await + 1;
        let access_ids: Vec<i32> = (0..number).map(|i| access_id + i as i32).collect();
        let refresh_tokens: Vec<String> = (0..number).map(|_| generate_token_string()).collect();
        let auth_tokens: Vec<String> = (0..number).map(|_| generate_token_string()).collect();
        let qs = token::insert_token(user_id, access_ids.clone(), refresh_tokens.iter().map(|rt| rt.as_str()).collect(), auth_tokens.iter().map(|rt| rt.as_str()).collect(), expired, ip);
        qs.execute(&self.pool).await?;
        Ok((0..number).map(|i| (access_ids[i], refresh_tokens[i].clone(), auth_tokens[i].clone())).collect())
    }

    pub async fn update_access_token(&self, access_id: i32, expired: Option<DateTime<Utc>>, ip: Option<&[u8]>)
        -> Result<String, Error>
    {
        let refresh_token = generate_token_string();
        let qs = token::update_token(TokenSelector::Access(access_id), Some(&refresh_token), expired, ip);
        qs.execute(&self.pool).await?;
        Ok(refresh_token)
    }

    pub async fn update_auth_token(&self, auth_token: &str, expired: Option<DateTime<Utc>>, ip: Option<&[u8]>)
        -> Result<String, Error>
    {
        let refresh_token = generate_token_string();
        let qs = token::update_token(TokenSelector::Auth(String::from(auth_token)), Some(&refresh_token), expired, ip);
        qs.execute(&self.pool).await?;
        Ok(refresh_token)
    }

    pub async fn delete_access_token(&self, access_id: i32)
        -> Result<(), Error>
    {
        let qs = token::delete_token(TokenSelector::Access(access_id));
        qs.execute(&self.pool).await
    }

    pub async fn delete_auth_token(&self, auth_token: &str)
        -> Result<(), Error>
    {
        let qs = token::delete_token(TokenSelector::Auth(String::from(auth_token)));
        qs.execute(&self.pool).await
    }

    pub async fn delete_token_by_user(&self, user_id: Uuid)
        -> Result<(), Error>
    {
        let qs = token::delete_token(TokenSelector::User(user_id));
        qs.execute(&self.pool).await
    }

}
