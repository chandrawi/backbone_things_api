pub mod api;
pub mod role;
pub mod user;
pub mod profile;
pub mod token;
pub mod auth;

use tonic::{Status, transport::Channel};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use bbthings_database::{
    ApiSchema, ProcedureSchema, RoleSchema, UserSchema,
    RoleProfileSchema, UserProfileSchema, TokenSchema,
    DataValue, DataType
};
use bbthings_grpc_server::proto::auth::auth::{UserLoginResponse, UserRefreshResponse, UserLogoutResponse};

#[derive(Debug, Clone)]
pub struct Auth {
    channel: Channel,
    pub user_id: Option<Uuid>,
    auth_token: String
}

impl Auth {

    pub async fn new(address: &str) -> Self {
        Self::new_with_token(address, &String::new()).await
    }

    pub async fn new_with_token(address: &str, auth_token: &str) -> Self {
        let channel = crate::utility::channel(address).await;
        Self::new_with_channel(channel, Some(auth_token))
    }

    pub fn new_with_channel(channel: Channel, auth_token: Option<&str>) -> Self {
        Self {
            channel,
            user_id: None,
            auth_token: auth_token.map(|at| String::from(at)).unwrap_or(String::new())
        }
    }

    pub async fn login(&mut self, username: &str, password: &str) -> Result<(), Status>
    {
        let login = auth::user_login(&self.channel, username, password).await?;
        self.auth_token = login.auth_token;
        self.user_id = Uuid::from_slice(&login.user_id).ok();
        Ok(())
    }

    pub async fn logout(&mut self) -> Result<(), Status>
    {
        if let Some(id) = self.user_id {
            auth::user_logout(&self.channel, id, &self.auth_token).await?;
            self.user_id = None;
            self.auth_token = String::new();
        }
        Ok(())
    }

    pub async fn user_login(&self, username: &str, password: &str)
        -> Result<UserLoginResponse, Status>
    {
        auth::user_login(&self.channel, username, password).await
    }

    pub async fn user_refresh(&self, api_id: Uuid, access_token: &str, refresh_token: &str)
        -> Result<UserRefreshResponse, Status>
    {
        auth::user_refresh(&self.channel, api_id, access_token, refresh_token).await
    }

    pub async fn user_logout(&self, user_id: Uuid, auth_token: &str)
        -> Result<UserLogoutResponse, Status>
    {
        auth::user_logout(&self.channel, user_id, auth_token).await
    }

    pub async fn read_api(&self, id: Uuid)
        -> Result<ApiSchema, Status>
    {
        api::read_api(&self, id).await
            .map(|s| s.into())
    }

    pub async fn read_api_by_name(&self, name: &str)
        -> Result<ApiSchema, Status>
    {
        api::read_api_by_name(&self, name).await
            .map(|s| s.into())
    }

    pub async fn list_api_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<ApiSchema>, Status>
    {
        api::list_api_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_api_by_name(&self, name: &str)
        -> Result<Vec<ApiSchema>, Status>
    {
        api::list_api_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_api_by_category(&self, category: &str)
        -> Result<Vec<ApiSchema>, Status>
    {
        api::list_api_by_category(&self, category).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_api_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<ApiSchema>, Status>
    {
        api::list_api_option(&self, name, category).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_api(&self, id: Uuid, name: &str, address: &str, category: &str, description: &str, password: &str, access_key: &[u8])
        -> Result<Uuid, Status>
    {
        api::create_api(&self, id, name, address, category, description, password, access_key)
            .await
    }

    pub async fn update_api(&self, id: Uuid, name: Option<&str>, address: Option<&str>, category: Option<&str>, description: Option<&str>, password: Option<&str>, access_key: Option<&[u8]>)
        -> Result<(), Status>
    {
        api::update_api(&self, id, name, address, category, description, password, access_key)
            .await
    }

    pub async fn delete_api(&self, id: Uuid)
        -> Result<(), Status>
    {
        api::delete_api(&self, id)
            .await
    }

    pub async fn read_procedure(&self, id: Uuid)
        -> Result<ProcedureSchema, Status>
    {
        api::read_procedure(&self, id).await
            .map(|s| s.into())
    }

    pub async fn read_procedure_by_name(&self, api_id: Uuid, name: &str)
        -> Result<ProcedureSchema, Status>
    {
        api::read_procedure_by_name(&self, api_id, name).await
            .map(|s| s.into())
    }

    pub async fn list_procedure_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<ProcedureSchema>, Status>
    {
        api::list_procedure_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_procedure_by_api(&self, api_id: Uuid)
        -> Result<Vec<ProcedureSchema>, Status>
    {
        api::list_procedure_by_api(&self, api_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_procedure_by_name(&self, name: &str)
        -> Result<Vec<ProcedureSchema>, Status>
    {
        api::list_procedure_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_procedure_option(&self, api_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<ProcedureSchema>, Status>
    {
        api::list_procedure_option(&self, api_id, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_procedure(&self, id: Uuid, api_id: Uuid, name: &str, description: &str)
        -> Result<Uuid, Status>
    {
        api::create_procedure(&self, id, api_id, name, description)
            .await
    }

    pub async fn update_procedure(&self, id: Uuid, name: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        api::update_procedure(&self, id, name, description)
            .await
    }

    pub async fn delete_procedure(&self, id: Uuid)
        -> Result<(), Status>
    {
        api::delete_procedure(&self, id)
            .await
    }

    pub async fn read_role(&self, id: Uuid)
        -> Result<RoleSchema, Status>
    {
        role::read_role(&self, id).await
            .map(|s| s.into())
    }

    pub async fn read_role_by_name(&self, api_id: Uuid, name: &str)
        -> Result<RoleSchema, Status>
    {
        role::read_role_by_name(&self, api_id, name).await
            .map(|s| s.into())
    }

    pub async fn list_role_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<RoleSchema>, Status>
    {
        role::list_role_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_role_by_api(&self, api_id: Uuid)
        -> Result<Vec<RoleSchema>, Status>
    {
        role::list_role_by_api(&self, api_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_role_by_user(&self, user_id: Uuid)
        -> Result<Vec<RoleSchema>, Status>
    {
        role::list_role_by_user(&self, user_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_role_by_name(&self, name: &str)
        -> Result<Vec<RoleSchema>, Status>
    {
        role::list_role_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_role_option(&self, api_id: Option<Uuid>, user_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<RoleSchema>, Status>
    {
        role::list_role_option(&self, api_id, user_id, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_role(&self, id: Uuid, api_id: Uuid, name: &str, multi: bool, ip_lock: bool, access_duration: i32, refresh_duration: i32)
        -> Result<Uuid, Status>
    {
        role::create_role(&self, id, api_id, name, multi, ip_lock, access_duration, refresh_duration)
            .await
    }

    pub async fn update_role(&self, id: Uuid, name: Option<&str>, multi: Option<bool>, ip_lock: Option<bool>, access_duration: Option<i32>, refresh_duration: Option<i32>)
        -> Result<(), Status>
    {
        role::update_role(&self, id, name, multi, ip_lock, access_duration, refresh_duration)
            .await
    }

    pub async fn delete_role(&self, id: Uuid)
        -> Result<(), Status>
    {
        role::delete_role(&self, id)
            .await
    }

    pub async fn add_role_access(&self, id: Uuid, procedure_id: Uuid)
        -> Result<(), Status>
    {
        role::add_role_access(&self, id, procedure_id)
            .await
    }

    pub async fn remove_role_access(&self, id: Uuid, procedure_id: Uuid)
        -> Result<(), Status>
    {
        role::remove_role_access(&self, id, procedure_id)
            .await
    }

    pub async fn read_role_profile(&self, id: i32)
        -> Result<RoleProfileSchema, Status>
    {
        profile::read_role_profile(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_role_profile_by_role(&self, role_id: Uuid)
        -> Result<Vec<RoleProfileSchema>, Status>
    {
        profile::list_role_profile_by_role(&self, role_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_role_profile(&self, role_id: Uuid, name: &str, value_type: DataType, value_default: DataValue, category: &str)
        -> Result<i32, Status>
    {
        profile::create_role_profile(&self, role_id, name, value_type, value_default, category)
            .await
    }

    pub async fn update_role_profile(&self, id: i32, name: Option<&str>, value_type: Option<DataType>, value_default: Option<DataValue>, category: Option<&str>)
        -> Result<(), Status>
    {
        profile::update_role_profile(&self, id, name, value_type, value_default, category)
            .await
    }

    pub async fn delete_role_profile(&self, id: i32)
        -> Result<(), Status>
    {
        profile::delete_role_profile(&self, id)
            .await
    }

    pub async fn read_user(&self, id: Uuid)
        -> Result<UserSchema, Status>
    {
        user::read_user(&self, id).await
            .map(|s| s.into())
    }

    pub async fn read_user_by_name(&self, name: &str)
        -> Result<UserSchema, Status>
    {
        user::read_user_by_name(&self, name).await
            .map(|s| s.into())
    }

    pub async fn list_user_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<UserSchema>, Status>
    {
        user::list_user_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_user_by_api(&self, api_id: Uuid)
        -> Result<Vec<UserSchema>, Status>
    {
        user::list_user_by_api(&self, api_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_user_by_role(&self, role_id: Uuid)
        -> Result<Vec<UserSchema>, Status>
    {
        user::list_user_by_role(&self, role_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_user_by_name(&self, name: &str)
        -> Result<Vec<UserSchema>, Status>
    {
        user::list_user_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_user_option(&self, api_id: Option<Uuid>, role_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<UserSchema>, Status>
    {
        user::list_user_option(&self, api_id, role_id, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_user(&self, id: Uuid, name: &str, email: &str, phone: &str, password: &str)
        -> Result<Uuid, Status>
    {
        user::create_user(&self, id, name, email, phone, password)
            .await
    }

    pub async fn update_user(&self, id: Uuid, name: Option<&str>, email: Option<&str>, phone: Option<&str>, password: Option<&str>)
        -> Result<(), Status>
    {
        user::update_user(&self, id, name, email, phone, password)
            .await
    }

    pub async fn delete_user(&self, id: Uuid)
        -> Result<(), Status>
    {
        user::delete_user(&self, id)
            .await
    }

    pub async fn add_user_role(&self, id: Uuid, role_id: Uuid)
        -> Result<(), Status>
    {
        user::add_user_role(&self, id, role_id)
            .await
    }

    pub async fn remove_user_role(&self, id: Uuid, role_id: Uuid)
        -> Result<(), Status>
    {
        user::remove_user_role(&self, id, role_id)
            .await
    }

    pub async fn read_user_profile(&self, id: i32)
        -> Result<UserProfileSchema, Status>
    {
        profile::read_user_profile(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_user_profile_by_user(&self, user_id: Uuid)
        -> Result<Vec<UserProfileSchema>, Status>
    {
        profile::list_user_profile_by_user(&self, user_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_user_profile(&self, user_id: Uuid, name: &str, value: DataValue, category: &str)
        -> Result<i32, Status>
    {
        profile::create_user_profile(&self, user_id, name, value, category)
            .await
    }

    pub async fn update_user_profile(&self, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
        -> Result<(), Status>
    {
        profile::update_user_profile(&self, id, name, value, category)
            .await
    }

    pub async fn delete_user_profile(&self, id: i32)
        -> Result<(), Status>
    {
        profile::delete_user_profile(&self, id)
            .await
    }

    pub async fn read_access_token(&self, access_id: i32)
        -> Result<TokenSchema, Status>
    {
        token::read_access_token(&self, access_id).await
            .map(|s| s.into())
    }

    pub async fn list_auth_token(&self, refresh_token: &str)
        -> Result<Vec<TokenSchema>, Status>
    {
        token::list_auth_token(&self, refresh_token).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_token_by_user(&self, user_id: Uuid)
        -> Result<Vec<TokenSchema>, Status>
    {
        token::list_token_by_user(&self, user_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_token_by_created_earlier(&self, earlier: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Status>
    {
        token::list_token_by_created_earlier(&self, earlier, user_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_token_by_created_later(&self, later: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Status>
    {
        token::list_token_by_created_later(&self, later, user_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_token_by_created_range(&self, begin: DateTime<Utc>, end: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Status>
    {
        token::list_token_by_created_range(&self, begin, end, user_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_token_by_expired_earlier(&self, earlier: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Status>
    {
        token::list_token_by_expired_earlier(&self, earlier, user_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_token_by_expired_later(&self, later: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Status>
    {
        token::list_token_by_expired_later(&self, later, user_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_token_by_expired_range(&self, begin: DateTime<Utc>, end: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Status>
    {
        token::list_token_by_expired_range(&self, begin, end, user_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_token_by_range(&self, b_created: DateTime<Utc>, e_created: DateTime<Utc>, b_expired: DateTime<Utc>, e_expired: DateTime<Utc>, user_id: Option<Uuid>)
        -> Result<Vec<TokenSchema>, Status>
    {
        token::list_token_by_range(&self, b_created, e_created, b_expired, e_expired, user_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_access_token(&self, user_id: Uuid, auth_token: &str, expire: DateTime<Utc>, ip: &[u8])
        -> Result<(i32, String, String), Status>
    {
        token::create_access_token(&self, user_id, auth_token, expire, ip)
            .await
    }

    pub async fn create_auth_token(&self, user_id: Uuid, expire: DateTime<Utc>, ip: &[u8], number: u32)
        -> Result<Vec<(i32, String, String)>, Status>
    {
        token::create_auth_token(&self, user_id, expire, ip, number)
            .await
    }

    pub async fn update_access_token(&self, access_id: i32, expire: Option<DateTime<Utc>>, ip: Option<&[u8]>)
        -> Result<String, Status>
    {
        token::update_access_token(&self, access_id, expire, ip)
            .await
    }

    pub async fn update_auth_token(&self, auth_token: &str, expire: Option<DateTime<Utc>>, ip: Option<&[u8]>)
        -> Result<String, Status>
    {
        token::update_auth_token(&self, auth_token, expire, ip)
            .await
    }

    pub async fn delete_access_token(&self, access_id: i32)
        -> Result<(), Status>
    {
        token::delete_access_token(&self, access_id)
            .await
    }

    pub async fn delete_auth_token(&self, auth_token: &str)
        -> Result<(), Status>
    {
        token::delete_auth_token(&self, auth_token)
            .await
    }

    pub async fn delete_token_by_user(&self, user_id: Uuid)
        -> Result<(), Status>
    {
        token::delete_token_by_user(&self, user_id)
            .await
    }

}
