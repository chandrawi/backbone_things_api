pub mod config;
pub mod model;
pub mod device;
pub mod group;
pub mod set;
pub mod data;
pub mod buffer;
pub mod slice;

use tonic::{Status, transport::Channel};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use bbthings_database::{
    ModelSchema, TagSchema, ModelConfigSchema,
    DeviceSchema, GatewaySchema, TypeSchema, DeviceConfigSchema, GatewayConfigSchema,
    GroupModelSchema, GroupDeviceSchema, GroupGatewaySchema, SetSchema, SetTemplateSchema,
    DataSchema, DataSetSchema, BufferSchema, BufferSetSchema, SliceSchema, SliceSetSchema,
    DataValue, DataType
};
use bbthings_grpc_server::proto::resource::config::{ProcedureAcces, RoleAcces};
use crate::auth::auth;

#[derive(Debug, Clone)]
pub struct Resource {
    channel: Channel,
    channel_auth: Option<Channel>,
    api_id: Uuid,
    user_id: Option<Uuid>,
    auth_token: String,
    access_token: String,
    refresh_token: String
}

impl Resource {

    pub async fn new(address: &str) -> Self {
        let channel = crate::utility::channel(address).await;
        let api_id = config::api_id(&channel).await.expect(config::API_ID_ERR);
        Self::new_with_channel(channel, Some(api_id), None, None, None)
    }

    pub async fn new_with_token(address: &str, api_id: Uuid, auth_token: &str, access_token: &str, refresh_token: &str) -> Self {
        let channel = crate::utility::channel(address).await;
        Self::new_with_channel(channel, Some(api_id), Some(auth_token), Some(access_token), Some(refresh_token))
    }

    pub fn new_with_channel(channel: Channel, api_id: Option<Uuid>, auth_token: Option<&str>, access_token: Option<&str>, refresh_token: Option<&str>) -> Self {
        Self {
            channel,
            channel_auth: None,
            api_id: api_id.unwrap_or(Uuid::nil()),
            user_id: None,
            auth_token: auth_token.map(|at| String::from(at)).unwrap_or(String::new()),
            access_token: access_token.map(|at| String::from(at)).unwrap_or(String::new()),
            refresh_token: refresh_token.map(|rt| String::from(rt)).unwrap_or(String::new())
        }
    }

    pub async fn login(&mut self, auth_address: &str, username: &str, password: &str) -> Result<(), Status>
    {
        let channel = crate::utility::channel(auth_address).await;
        let login = auth::user_login(&channel, username, password).await?;
        self.channel_auth = Some(channel);
        self.auth_token = login.auth_token;
        for map in login.access_tokens {
            if map.api_id == self.api_id.as_bytes().to_vec() {
                self.access_token = map.access_token;
                self.refresh_token = map.refresh_token;
            }
        }
        self.user_id = Uuid::from_slice(&login.user_id).ok();
        Ok(())
    }

    pub async fn refresh(&mut self) -> Result<(), Status>
    {
        if let Some(channel) = self.channel_auth.clone() {
            let refresh = auth::user_refresh(&channel, self.api_id, &self.access_token, &self.refresh_token).await?;
            self.access_token = refresh.access_token;
            self.refresh_token = refresh.refresh_token;
        }
        Ok(())
    }

    pub async fn logout(&self) -> Result<(), Status>
    {
        if let (Some(channel), Some(id)) = (self.channel_auth.clone(), self.user_id) {
            auth::user_logout(&channel, id, &self.auth_token).await?;
        }
        Ok(())
    }

    pub async fn api_id(&self) -> Result<Uuid, Status>
    {
        config::api_id(&self.channel).await
    }

    pub async fn procedure_access(&self) -> Result<Vec<ProcedureAcces>, Status>
    {
        config::procedure_access(&self).await
    }

    pub async fn role_access(&self) -> Result<Vec<RoleAcces>, Status>
    {
        config::role_access(&self).await
    }

    pub async fn read_model(&self, id: Uuid)
        -> Result<ModelSchema, Status>
    {
        model::read_model(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_model_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<ModelSchema>, Status>
    {
        model::list_model_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_model_by_type(&self, type_id: Uuid)
        -> Result<Vec<ModelSchema>, Status>
    {
        model::list_model_by_type(&self, type_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_model_by_name(&self, name: &str)
        -> Result<Vec<ModelSchema>, Status>
    {
        model::list_model_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_model_by_category(&self, category: &str)
        -> Result<Vec<ModelSchema>, Status>
    {
        model::list_model_by_category(&self, category).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_model_option(&self, type_id: Option<Uuid>, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<ModelSchema>, Status>
    {
        model::list_model_option(&self, type_id, name, category).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_model(&self, id: Uuid, data_type: &[DataType], category: &str, name: &str, description: Option<&str>)
        -> Result<Uuid, Status>
    {
        model::create_model(&self, id, data_type, category, name, description)
            .await
    }

    pub async fn update_model(&self, id: Uuid, data_type: Option<&[DataType]>, category: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        model::update_model(&self, id, data_type, category, name, description)
            .await
    }

    pub async fn delete_model(&self, id: Uuid)
        -> Result<(), Status>
    {
        model::delete_model(&self, id)
            .await
    }

    pub async fn read_model_config(&self, id: i32)
        -> Result<ModelConfigSchema, Status>
    {
        model::read_model_config(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_model_config_by_model(&self, model_id: Uuid)
        -> Result<Vec<ModelConfigSchema>, Status>
    {
        model::list_model_config_by_model(&self, model_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_model_config(&self, model_id: Uuid, index: i32, name: &str, value: DataValue, category: &str)
        -> Result<i32, Status>
    {
        model::create_model_config(&self, model_id, index, name, value, category)
            .await
    }

    pub async fn update_model_config(&self, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
        -> Result<(), Status>
    {
        model::update_model_config(&self, id, name, value, category)
            .await
    }

    pub async fn delete_model_config(&self, id: i32)
        -> Result<(), Status>
    {
        model::delete_model_config(&self, id)
            .await
    }

    pub async fn read_tag(&self, model_id: Uuid, tag: i16)
        -> Result<TagSchema, Status>
    {
        model::read_tag(&self, model_id, tag).await
            .map(|s| s.into())
    }

    pub async fn list_tag_by_model(&self, model_id: Uuid)
        -> Result<Vec<TagSchema>, Status>
    {
        model::list_tag_by_model(&self, model_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_tag(&self, model_id: Uuid, tag: i16, name: &str, members: &[i16])
        -> Result<(), Status>
    {
        model::create_tag(&self, model_id, tag, name, members)
            .await
    }

    pub async fn update_tag(&self, model_id: Uuid, tag: i16, name: Option<&str>, members: Option<&[i16]>)
        -> Result<(), Status>
    {
        model::update_tag(&self, model_id, tag, name, members)
            .await
    }

    pub async fn delete_tag(&self, model_id: Uuid, tag: i16)
        -> Result<(), Status>
    {
        model::delete_tag(&self, model_id, tag)
            .await
    }

    pub async fn read_device(&self, id: Uuid)
        -> Result<DeviceSchema, Status>
    {
        device::read_device(&self, id).await
            .map(|s| s.into())
    }

    pub async fn read_device_by_sn(&self, serial_number: &str)
        -> Result<DeviceSchema, Status>
    {
        device::read_device_by_sn(&self, serial_number).await
            .map(|s| s.into())
    }

    pub async fn list_device_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<DeviceSchema>, Status>
    {
        device::list_device_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_device_by_gateway(&self, gateway_id: Uuid)
        -> Result<Vec<DeviceSchema>, Status>
    {
        device::list_device_by_gateway(&self, gateway_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_device_by_type(&self, type_id: Uuid)
        -> Result<Vec<DeviceSchema>, Status>
    {
        device::list_device_by_type(&self, type_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_device_by_name(&self, name: &str)
        -> Result<Vec<DeviceSchema>, Status>
    {
        device::list_device_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_device_option(&self, gateway_id: Option<Uuid>, type_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<DeviceSchema>, Status>
    {
        device::list_device_option(&self, gateway_id, type_id, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_device(&self, id: Uuid, gateway_id: Uuid, type_id: Uuid, serial_number: &str, name: &str, description: Option<&str>)
        -> Result<Uuid, Status>
    {
        device::create_device(&self, id, gateway_id, type_id, serial_number, name, description)
            .await
    }

    pub async fn update_device(&self, id: Uuid, gateway_id: Option<Uuid>, type_id: Option<Uuid>, serial_number: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        device::update_device(&self, id, gateway_id, type_id, serial_number, name, description)
            .await
    }

    pub async fn delete_device(&self, id: Uuid)
        -> Result<(), Status>
    {
        device::delete_device(&self, id)
            .await
    }

    pub async fn read_gateway(&self, id: Uuid)
        -> Result<GatewaySchema, Status>
    {
        device::read_gateway(&self, id).await
            .map(|s| s.into())
    }

    pub async fn read_gateway_by_sn(&self, serial_number: &str)
        -> Result<GatewaySchema, Status>
    {
        device::read_gateway_by_sn(&self, serial_number).await
            .map(|s| s.into())
    }

    pub async fn list_gateway_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GatewaySchema>, Status>
    {
        device::list_gateway_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_gateway_by_type(&self, type_id: Uuid)
        -> Result<Vec<GatewaySchema>, Status>
    {
        device::list_gateway_by_type(&self, type_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_gateway_by_name(&self, name: &str)
        -> Result<Vec<GatewaySchema>, Status>
    {
        device::list_gateway_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_gateway_option(&self, type_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<GatewaySchema>, Status>
    {
        device::list_gateway_option(&self, type_id, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_gateway(&self, id: Uuid, type_id: Uuid, serial_number: &str, name: &str, description: Option<&str>)
        -> Result<Uuid, Status>
    {
        device::create_gateway(&self, id, type_id, serial_number, name, description)
            .await
    }

    pub async fn update_gateway(&self, id: Uuid, type_id: Option<Uuid>, serial_number: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        device::update_gateway(&self, id, type_id, serial_number, name, description)
            .await
    }

    pub async fn delete_gateway(&self, id: Uuid)
        -> Result<(), Status>
    {
        device::delete_gateway(&self, id)
            .await
    }

    pub async fn read_device_config(&self, id: i32)
        -> Result<DeviceConfigSchema, Status>
    {
        device::read_device_config(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_device_config_by_device(&self, device_id: Uuid)
        -> Result<Vec<DeviceConfigSchema>, Status>
    {
        device::list_device_config_by_device(&self, device_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_device_config(&self, device_id: Uuid, name: &str, value: DataValue, category: &str)
        -> Result<i32, Status>
    {
        device::create_device_config(&self, device_id, name, value, category)
            .await
    }

    pub async fn update_device_config(&self, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
        -> Result<(), Status>
    {
        device::update_device_config(&self, id, name, value, category)
            .await
    }

    pub async fn delete_device_config(&self, id: i32)
        -> Result<(), Status>
    {
        device::delete_device_config(&self, id)
            .await
    }

    pub async fn read_gateway_config(&self, id: i32)
        -> Result<GatewayConfigSchema, Status>
    {
        device::read_gateway_config(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_gateway_config_by_gateway(&self, gateway_id: Uuid)
        -> Result<Vec<GatewayConfigSchema>, Status>
    {
        device::list_gateway_config_by_gateway(&self, gateway_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_gateway_config(&self, gateway_id: Uuid, name: &str, value: DataValue, category: &str)
        -> Result<i32, Status>
    {
        device::create_gateway_config(&self, gateway_id, name, value, category)
            .await
    }

    pub async fn update_gateway_config(&self, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
        -> Result<(), Status>
    {
        device::update_gateway_config(&self, id, name, value, category)
            .await
    }

    pub async fn delete_gateway_config(&self, id: i32)
        -> Result<(), Status>
    {
        device::delete_gateway_config(&self, id)
            .await
    }

    pub async fn read_type(&self, id: Uuid)
        -> Result<TypeSchema, Status>
    {
        device::read_type(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_type_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<TypeSchema>, Status>
    {
        device::list_type_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_type_by_name(&self, name: &str)
        -> Result<Vec<TypeSchema>, Status>
    {
        device::list_type_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_type_option(&self, name: Option<&str>)
        -> Result<Vec<TypeSchema>, Status>
    {
        device::list_type_option(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_type(&self, id: Uuid, name: &str, description: Option<&str>)
        -> Result<Uuid, Status>
    {
        device::create_type(&self, id, name, description)
            .await
    }

    pub async fn update_type(&self, id: Uuid, name: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        device::update_type(&self, id, name, description)
            .await
    }

    pub async fn delete_type(&self, id: Uuid)
        -> Result<(), Status>
    {
        device::delete_type(&self, id)
            .await
    }

    pub async fn add_type_model(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Status>
    {
        device::add_type_model(&self, id, model_id)
            .await
    }

    pub async fn remove_type_model(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Status>
    {
        device::remove_type_model(&self, id, model_id)
            .await
    }

    pub async fn read_group_model(&self, id: Uuid)
        -> Result<GroupModelSchema, Status>
    {
        group::read_group_model(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_group_model_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GroupModelSchema>, Status>
    {
        group::list_group_model_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_group_model_by_name(&self, name: &str)
        -> Result<Vec<GroupModelSchema>, Status>
    {
        group::list_group_model_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_group_model_by_category(&self, category: &str)
        -> Result<Vec<GroupModelSchema>, Status>
    {
        group::list_group_model_by_category(&self, category).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_group_model_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<GroupModelSchema>, Status>
    {
        group::list_group_model_option(&self, name, category).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_group_model(&self, id: Uuid, name: &str, category: &str, description: Option<&str>)
        -> Result<Uuid, Status>
    {
        group::create_group_model(&self, id, name, category, description)
            .await
    }

    pub async fn update_group_model(&self, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        group::update_group_model(&self, id, name, category, description)
            .await
    }

    pub async fn delete_group_model(&self, id: Uuid)
        -> Result<(), Status>
    {
        group::delete_group_model(&self, id)
            .await
    }

    pub async fn add_group_model_member(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Status>
    {
        group::add_group_model_member(&self, id, model_id)
            .await
    }

    pub async fn remove_group_model_member(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Status>
    {
        group::remove_group_model_member(&self, id, model_id)
            .await
    }

    pub async fn read_group_device(&self, id: Uuid)
        -> Result<GroupDeviceSchema, Status>
    {
        group::read_group_device(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_group_device_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GroupDeviceSchema>, Status>
    {
        group::list_group_device_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_group_device_by_name(&self, name: &str)
        -> Result<Vec<GroupDeviceSchema>, Status>
    {
        group::list_group_device_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_group_device_by_category(&self, category: &str)
        -> Result<Vec<GroupDeviceSchema>, Status>
    {
        group::list_group_device_by_category(&self, category).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_group_device_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<GroupDeviceSchema>, Status>
    {
        group::list_group_device_option(&self, name, category).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_group_device(&self, id: Uuid, name: &str, category: &str, description: Option<&str>)
        -> Result<Uuid, Status>
    {
        group::create_group_device(&self, id, name, category, description)
            .await
    }

    pub async fn update_group_device(&self, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        group::update_group_device(&self, id, name, category, description)
            .await
    }

    pub async fn delete_group_device(&self, id: Uuid)
        -> Result<(), Status>
    {
        group::delete_group_device(&self, id)
            .await
    }

    pub async fn add_group_device_member(&self, id: Uuid, device_id: Uuid)
        -> Result<(), Status>
    {
        group::add_group_device_member(&self, id, device_id)
            .await
    }

    pub async fn remove_group_device_member(&self, id: Uuid, device_id: Uuid)
        -> Result<(), Status>
    {
        group::remove_group_device_member(&self, id, device_id)
            .await
    }

    pub async fn read_group_gateway(&self, id: Uuid)
        -> Result<GroupGatewaySchema, Status>
    {
        group::read_group_gateway(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_group_gateway_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GroupGatewaySchema>, Status>
    {
        group::list_group_gateway_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_group_gateway_by_name(&self, name: &str)
        -> Result<Vec<GroupGatewaySchema>, Status>
    {
        group::list_group_gateway_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_group_gateway_by_category(&self, category: &str)
        -> Result<Vec<GroupGatewaySchema>, Status>
    {
        group::list_group_gateway_by_category(&self, category).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_group_gateway_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<GroupGatewaySchema>, Status>
    {
        group::list_group_gateway_option(&self, name, category).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_group_gateway(&self, id: Uuid, name: &str, category: &str, description: Option<&str>)
        -> Result<Uuid, Status>
    {
        group::create_group_gateway(&self, id, name, category, description)
            .await
    }

    pub async fn update_group_gateway(&self, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        group::update_group_gateway(&self, id, name, category, description)
            .await
    }

    pub async fn delete_group_gateway(&self, id: Uuid)
        -> Result<(), Status>
    {
        group::delete_group_gateway(&self, id)
            .await
    }

    pub async fn add_group_gateway_member(&self, id: Uuid, gateway_id: Uuid)
        -> Result<(), Status>
    {
        group::add_group_gateway_member(&self, id, gateway_id)
            .await
    }

    pub async fn remove_group_gateway_member(&self, id: Uuid, gateway_id: Uuid)
        -> Result<(), Status>
    {
        group::remove_group_gateway_member(&self, id, gateway_id)
            .await
    }

    pub async fn read_set(&self, id: Uuid)
        -> Result<SetSchema, Status>
    {
        set::read_set(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_set_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<SetSchema>, Status>
    {
        set::list_set_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_set_by_template(&self, template_id: Uuid)
        -> Result<Vec<SetSchema>, Status>
    {
        set::list_set_by_template(&self, template_id).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_set_by_name(&self, name: &str)
        -> Result<Vec<SetSchema>, Status>
    {
        set::list_set_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_set_option(&self, template_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<SetSchema>, Status>
    {
        set::list_set_option(&self, template_id, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_set(&self, id: Uuid, template_id: Uuid, name: &str, description: Option<&str>)
        -> Result<Uuid, Status>
    {
        set::create_set(&self, id, template_id, name, description)
            .await
    }

    pub async fn update_set(&self, id: Uuid, template_id: Option<Uuid>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        set::update_set(&self, id, template_id, name, description)
            .await
    }

    pub async fn delete_set(&self, id: Uuid)
        -> Result<(), Status>
    {
        set::delete_set(&self, id)
            .await
    }

    pub async fn add_set_member(&self, id: Uuid, device_id: Uuid, model_id: Uuid, data_index: &[u8])
        -> Result<(), Status>
    {
        set::add_set_member(&self, id, device_id, model_id, data_index)
            .await
    }

    pub async fn remove_set_member(&self, id: Uuid, device_id: Uuid, model_id: Uuid)
        -> Result<(), Status>
    {
        set::remove_set_member(&self, id, device_id, model_id)
            .await
    }

    pub async fn swap_set_member(&self, id: Uuid, device_id_1: Uuid, model_id_1: Uuid, device_id_2: Uuid, model_id_2: Uuid)
        -> Result<(), Status>
    {
        set::swap_set_member(&self, id, device_id_1, model_id_1, device_id_2, model_id_2)
            .await
    }

    pub async fn read_set_template(&self, id: Uuid)
        -> Result<SetTemplateSchema, Status>
    {
        set::read_set_template(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_set_template_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<SetTemplateSchema>, Status>
    {
        set::list_set_template_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_set_template_by_name(&self, name: &str)
        -> Result<Vec<SetTemplateSchema>, Status>
    {
        set::list_set_template_by_name(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_set_template_option(&self, name: Option<&str>)
        -> Result<Vec<SetTemplateSchema>, Status>
    {
        set::list_set_template_option(&self, name).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_set_template(&self, id: Uuid, name: &str, description: Option<&str>)
        -> Result<Uuid, Status>
    {
        set::create_set_template(&self, id, name, description)
            .await
    }

    pub async fn update_set_template(&self, id: Uuid, name: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        set::update_set_template(&self, id, name, description)
            .await
    }

    pub async fn delete_set_template(&self, id: Uuid)
        -> Result<(), Status>
    {
        set::delete_set_template(&self, id)
            .await
    }

    pub async fn add_set_template_member(&self, id: Uuid, type_id: Uuid, model_id: Uuid, data_index: &[u8])
        -> Result<(), Status>
    {
        set::add_set_template_member(&self, id, type_id, model_id, data_index)
            .await
    }

    pub async fn remove_set_template_member(&self, id: Uuid, index: usize)
        -> Result<(), Status>
    {
        set::remove_set_template_member(&self, id, index)
            .await
    }

    pub async fn swap_set_template_member(&self, id: Uuid, index_1: usize, index_2: usize)
        -> Result<(), Status>
    {
        set::swap_set_template_member(&self, id, index_1, index_2)
            .await
    }

    pub async fn read_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DataSchema, Status>
    {
        data::read_data(&self, device_id, model_id, timestamp, tag).await
            .map(|s| s.into())
    }

    pub async fn list_data_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_by_time(&self, device_id, model_id, timestamp, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_by_earlier(&self, device_id, model_id, earlier, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_by_later(&self, device_id, model_id, later, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_by_range(&self, device_id, model_id, begin, end, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_by_number_before(&self, device_id: Uuid, model_id: Uuid, before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_by_number_before(&self, device_id, model_id, before, number, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_by_number_after(&self, device_id: Uuid, model_id: Uuid, after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_by_number_after(&self, device_id, model_id, after, number, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_group_by_time(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_group_by_time(&self, device_ids, model_ids, timestamp, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_group_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_group_by_earlier(&self, device_ids, model_ids, earlier, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_group_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_group_by_later(&self, device_ids, model_ids, later, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_group_by_range(&self, device_ids, model_ids, begin, end, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_group_by_number_before(&self, device_ids: &[Uuid], model_ids: &[Uuid], before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_group_by_number_before(&self, device_ids, model_ids, before, number, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_group_by_number_after(&self, device_ids: &[Uuid], model_ids: &[Uuid], after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Status>
    {
        data::list_data_group_by_number_after(&self, device_ids, model_ids, after, number, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn read_data_set(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DataSetSchema, Status>
    {
        data::read_data_set(&self, set_id, timestamp, tag).await
            .map(|s| s.into())
    }

    pub async fn list_data_set_by_time(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Status>
    {
        data::list_data_set_by_time(&self, set_id, timestamp, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_set_by_earlier(&self, set_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Status>
    {
        data::list_data_set_by_earlier(&self, set_id, earlier, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_set_by_later(&self, set_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Status>
    {
        data::list_data_set_by_later(&self, set_id, later, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_data_set_by_range(&self, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Status>
    {
        data::list_data_set_by_range(&self, set_id, begin, end, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: &[DataValue], tag: Option<i16>)
        -> Result<(), Status>
    {
        data::create_data(&self, device_id, model_id, timestamp, data, tag)
            .await
    }

    pub async fn create_data_multiple(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamps: &[DateTime<Utc>], data: &[&[DataValue]], tags: Option<&[i16]>)
        -> Result<(), Status>
    {
        data::create_data_multiple(&self, device_ids, model_ids, timestamps, data, tags)
            .await
    }

    pub async fn delete_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<(), Status>
    {
        data::delete_data(&self, device_id, model_id, timestamp, tag)
            .await
    }

    pub async fn read_data_timestamp(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Status>
    {
        data::read_data_timestamp(&self, device_id, model_id, timestamp, tag)
            .await
    }

    pub async fn list_data_timestamp_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        data::list_data_timestamp_by_earlier(&self, device_id, model_id, earlier, tag)
            .await
    }

    pub async fn list_data_timestamp_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        data::list_data_timestamp_by_later(&self, device_id, model_id, later, tag)
            .await
    }

    pub async fn list_data_timestamp_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        data::list_data_timestamp_by_range(&self, device_id, model_id, begin, end, tag)
            .await
    }

    pub async fn read_data_group_timestamp(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Status>
    {
        data::read_data_group_timestamp(&self, device_ids, model_ids, timestamp, tag)
            .await
    }

    pub async fn list_data_group_timestamp_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        data::list_data_group_timestamp_by_earlier(&self, device_ids, model_ids, earlier, tag)
            .await
    }

    pub async fn list_data_group_timestamp_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        data::list_data_group_timestamp_by_later(&self, device_ids, model_ids, later, tag)
            .await
    }

    pub async fn list_data_group_timestamp_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        data::list_data_group_timestamp_by_range(&self, device_ids, model_ids, begin, end, tag)
            .await
    }

    pub async fn count_data(&self, device_id: Uuid, model_id: Uuid, tag: Option<i16>)
        -> Result<usize, Status>
    {
        data::count_data(&self, device_id, model_id, tag)
            .await
    }

    pub async fn count_data_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        data::count_data_by_earlier(&self, device_id, model_id, earlier, tag)
            .await
    }

    pub async fn count_data_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        data::count_data_by_later(&self, device_id, model_id, later, tag)
            .await
    }

    pub async fn count_data_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        data::count_data_by_range(&self, device_id, model_id, begin, end, tag)
            .await
    }

    pub async fn count_data_group(&self, device_ids: &[Uuid], model_ids: &[Uuid], tag: Option<i16>)
        -> Result<usize, Status>
    {
        data::count_data_group(&self, device_ids, model_ids, tag)
            .await
    }

    pub async fn count_data_group_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        data::count_data_group_by_earlier(&self, device_ids, model_ids, earlier, tag)
            .await
    }

    pub async fn count_data_group_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        data::count_data_group_by_later(&self, device_ids, model_ids, later, tag)
            .await
    }

    pub async fn count_data_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        data::count_data_group_by_range(&self, device_ids, model_ids, begin, end, tag)
            .await
    }

    pub async fn read_buffer(&self, id: i32)
        -> Result<BufferSchema, Status>
    {
        buffer::read_buffer(&self, id).await
            .map(|s| s.into())
    }

    pub async fn read_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<BufferSchema, Status>
    {
        buffer::read_buffer_by_time(&self, device_id, model_id, timestamp, tag).await
            .map(|s| s.into())
    }

    pub async fn list_buffer_by_ids(&self, ids: &[i32])
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_by_time(&self, device_id, model_id, timestamp, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_by_earlier(&self, device_id, model_id, earlier, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_by_later(&self, device_id, model_id, later, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_by_range(&self, device_id, model_id, begin, end, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_by_number_before(&self, device_id: Uuid, model_id: Uuid, before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_by_number_before(&self, device_id, model_id, before, number, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_by_number_after(&self, device_id: Uuid, model_id: Uuid, after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_by_number_after(&self, device_id, model_id, after, number, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn read_buffer_first(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<BufferSchema, Status>
    {
        buffer::read_buffer_first(&self, device_id, model_id, tag).await
            .map(|s| s.into())
    }

    pub async fn read_buffer_last(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<BufferSchema, Status>
    {
        buffer::read_buffer_last(&self, device_id, model_id, tag).await
            .map(|s| s.into())
    }

    pub async fn list_buffer_first(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_first(&self, number, device_id, model_id, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_first_offset(&self, number: usize, offset: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_first_offset(&self, number, offset, device_id, model_id, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_last(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_last(&self, number, device_id, model_id, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_last_offset(&self, number: usize, offset: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_last_offset(&self, number, offset, device_id, model_id, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_group_by_time(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_group_by_time(&self, device_ids, model_ids, timestamp, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_group_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_group_by_earlier(&self, device_ids, model_ids, earlier, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_group_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_group_by_later(&self, device_ids, model_ids, later, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_group_by_range(&self, device_ids, model_ids, begin, end, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_group_by_number_before(&self, device_ids: &[Uuid], model_ids: &[Uuid], before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_group_by_number_before(&self, device_ids, model_ids, before, number, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_group_by_number_after(&self, device_ids: &[Uuid], model_ids: &[Uuid], after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_group_by_number_after(&self, device_ids, model_ids, after, number, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn read_buffer_group_first(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<BufferSchema, Status>
    {
        buffer::read_buffer_group_first(&self, device_ids, model_ids, tag).await
            .map(|s| s.into())
    }

    pub async fn read_buffer_group_last(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<BufferSchema, Status>
    {
        buffer::read_buffer_group_last(&self, device_ids, model_ids, tag).await
            .map(|s| s.into())
    }

    pub async fn list_buffer_group_first(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_group_first(&self, number, device_ids, model_ids, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_group_first_offset(&self, number: usize, offset: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_group_first_offset(&self, number, offset, device_ids, model_ids, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_group_last(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_group_last(&self, number, device_ids, model_ids, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_group_last_offset(&self, number: usize, offset: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Status>
    {
        buffer::list_buffer_group_last_offset(&self, number, offset, device_ids, model_ids, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn read_buffer_set(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<BufferSetSchema, Status>
    {
        buffer::read_buffer_set(&self, set_id, timestamp, tag).await
            .map(|s| s.into())
    }

    pub async fn list_buffer_set_by_time(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Status>
    {
        buffer::list_buffer_set_by_time(&self, set_id, timestamp, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_set_by_earlier(&self, set_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Status>
    {
        buffer::list_buffer_set_by_earlier(&self, set_id, earlier, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_set_by_later(&self, set_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Status>
    {
        buffer::list_buffer_set_by_later(&self, set_id, later, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_buffer_set_by_range(&self, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Status>
    {
        buffer::list_buffer_set_by_range(&self, set_id, begin, end, tag).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_buffer(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: &[DataValue], tag: Option<i16>)
        -> Result<i32, Status>
    {
        buffer::create_buffer(&self, device_id, model_id, timestamp, data, tag)
            .await
    }

    pub async fn create_buffer_multiple(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamps: &[DateTime<Utc>], data: &[&[DataValue]], tags: Option<&[i16]>)
        -> Result<Vec<i32>, Status>
    {
        buffer::create_buffer_multiple(&self, device_ids, model_ids, timestamps, data, tags)
            .await
    }

    pub async fn update_buffer(&self, id: i32, data: Option<&[DataValue]>, tag: Option<i16>)
        -> Result<(), Status>
    {
        buffer::update_buffer(&self, id, data, tag.map(|s| s.into()))
            .await
    }

    pub async fn update_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: Option<&[DataValue]>, tag: Option<i16>)
        -> Result<(), Status>
    {
        buffer::update_buffer_by_time(&self, device_id, model_id, timestamp, data, tag)
            .await
    }

    pub async fn delete_buffer(&self, id: i32)
        -> Result<(), Status>
    {
        buffer::delete_buffer(&self, id)
            .await
    }

    pub async fn delete_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<(), Status>
    {
        buffer::delete_buffer_by_time(&self, device_id, model_id, timestamp, tag)
            .await
    }

    pub async fn read_buffer_timestamp(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Status>
    {
        buffer::read_buffer_timestamp(&self, device_id, model_id, timestamp, tag)
            .await
    }

    pub async fn list_buffer_timestamp_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        buffer::list_buffer_timestamp_by_earlier(&self, device_id, model_id, earlier, tag)
            .await
    }

    pub async fn list_buffer_timestamp_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        buffer::list_buffer_timestamp_by_later(&self, device_id, model_id, later, tag)
            .await
    }

    pub async fn list_buffer_timestamp_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        buffer::list_buffer_timestamp_by_range(&self, device_id, model_id, begin, end, tag)
            .await
    }

    pub async fn list_buffer_timestamp_first(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        buffer::list_buffer_timestamp_first(&self, number, device_id, model_id, tag)
            .await
    }

    pub async fn list_buffer_timestamp_last(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        buffer::list_buffer_timestamp_last(&self, number, device_id, model_id, tag)
            .await
    }

    pub async fn read_buffer_group_timestamp(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Status>
    {
        buffer::read_buffer_group_timestamp(&self, device_ids, model_ids, timestamp, tag)
            .await
    }

    pub async fn list_buffer_group_timestamp_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        buffer::list_buffer_group_timestamp_by_earlier(&self, device_ids, model_ids, earlier, tag)
            .await
    }

    pub async fn list_buffer_group_timestamp_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        buffer::list_buffer_group_timestamp_by_later(&self, device_ids, model_ids, later, tag)
            .await
    }

    pub async fn list_buffer_group_timestamp_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        buffer::list_buffer_group_timestamp_by_range(&self, device_ids, model_ids, begin, end, tag)
            .await
    }

    pub async fn list_buffer_group_timestamp_first(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        buffer::list_buffer_group_timestamp_first(&self, number, device_ids, model_ids, tag)
            .await
    }

    pub async fn list_buffer_group_timestamp_last(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Status>
    {
        buffer::list_buffer_group_timestamp_last(&self, number, device_ids, model_ids, tag)
            .await
    }

    pub async fn count_buffer(&self, device_id: Uuid, model_id: Uuid, tag: Option<i16>)
        -> Result<usize, Status>
    {
        buffer::count_buffer(&self, device_id, model_id, tag)
            .await
    }

    pub async fn count_buffer_by_earlier(&self, device_id: Uuid, model_id: Uuid, earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        buffer::count_buffer_by_earlier(&self, device_id, model_id, earlier, tag)
            .await
    }

    pub async fn count_buffer_by_later(&self, device_id: Uuid, model_id: Uuid, later: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        buffer::count_buffer_by_later(&self, device_id, model_id, later, tag)
            .await
    }

    pub async fn count_buffer_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        buffer::count_buffer_by_range(&self, device_id, model_id, begin, end, tag)
            .await
    }

    pub async fn count_buffer_group(&self, device_ids: &[Uuid], model_ids: &[Uuid], tag: Option<i16>)
        -> Result<usize, Status>
    {
        buffer::count_buffer_group(&self, device_ids, model_ids, tag)
            .await
    }

    pub async fn count_buffer_group_by_earlier(&self, device_ids: &[Uuid], model_ids: &[Uuid], earlier: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        buffer::count_buffer_group_by_earlier(&self, device_ids, model_ids, earlier, tag)
            .await
    }

    pub async fn count_buffer_group_by_later(&self, device_ids: &[Uuid], model_ids: &[Uuid], later: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        buffer::count_buffer_group_by_later(&self, device_ids, model_ids, later, tag)
            .await
    }

    pub async fn count_buffer_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Status>
    {
        buffer::count_buffer_group_by_range(&self, device_ids, model_ids, begin, end, tag)
            .await
    }

    pub async fn read_slice(&self, id: i32)
        -> Result<SliceSchema, Status>
    {
        slice::read_slice(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_slice_by_ids(&self, ids: &[i32])
        -> Result<Vec<SliceSchema>, Status>
    {
        slice::list_slice_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())

    }

    pub async fn list_slice_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Status>
    {
        slice::list_slice_by_time(&self, device_id, model_id, timestamp).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Status>
    {
        slice::list_slice_by_range(&self, device_id, model_id, begin, end).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_by_name_time(&self, name: &str, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Status>
    {
        slice::list_slice_by_name_time(&self, name, timestamp).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_by_name_range(&self, name: &str, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Status>
    {
        slice::list_slice_by_name_range(&self, name, begin, end).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_option(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
        -> Result<Vec<SliceSchema>, Status>
    {
        slice::list_slice_option(&self, device_id, model_id, name, begin_or_timestamp, end).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_group_by_time(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Status>
    {
        slice::list_slice_group_by_time(&self, device_ids, model_ids, timestamp).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Status>
    {
        slice::list_slice_group_by_range(&self, device_ids, model_ids, begin, end).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_group_option(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
        -> Result<Vec<SliceSchema>, Status>
    {
        slice::list_slice_group_option(&self, device_ids, model_ids, name, begin_or_timestamp, end).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_slice(&self, device_id: Uuid, model_id: Uuid, timestamp_begin: DateTime<Utc>, timestamp_end: DateTime<Utc>, name: &str, description: Option<&str>)
        -> Result<i32, Status>
    {
        slice::create_slice(&self, device_id, model_id, timestamp_begin, timestamp_end, name, description)
            .await
    }

    pub async fn update_slice(&self, id: i32, timestamp_begin: Option<DateTime<Utc>>, timestamp_end: Option<DateTime<Utc>>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        slice::update_slice(&self, id, timestamp_begin, timestamp_end, name, description)
            .await
    }

    pub async fn delete_slice(&self, id: i32)
        -> Result<(), Status>
    {
        slice::delete_slice(&self, id)
            .await
    }

    pub async fn read_slice_set(&self, id: i32)
        -> Result<SliceSetSchema, Status>
    {
        slice::read_slice_set(&self, id).await
            .map(|s| s.into())
    }

    pub async fn list_slice_set_by_ids(&self, ids: &[i32])
        -> Result<Vec<SliceSetSchema>, Status>
    {
        slice::list_slice_set_by_ids(&self, ids).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())

    }

    pub async fn list_slice_set_by_time(&self, set_id: Uuid, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Status>
    {
        slice::list_slice_set_by_time(&self, set_id, timestamp).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_set_by_range(&self, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Status>
    {
        slice::list_slice_set_by_range(&self, set_id, begin, end).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_set_by_name_time(&self, name: &str, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Status>
    {
        slice::list_slice_set_by_name_time(&self, name, timestamp).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_set_by_name_range(&self, name: &str, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Status>
    {
        slice::list_slice_set_by_name_range(&self, name, begin, end).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn list_slice_set_option(&self, set_id: Option<Uuid>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
        -> Result<Vec<SliceSetSchema>, Status>
    {
        slice::list_slice_set_option(&self, set_id, name, begin_or_timestamp, end).await
            .map(|v| v.into_iter().map(|s| s.into()).collect())
    }

    pub async fn create_slice_set(&self, set_id: Uuid, timestamp_begin: DateTime<Utc>, timestamp_end: DateTime<Utc>, name: &str, description: Option<&str>)
        -> Result<i32, Status>
    {
        slice::create_slice_set(&self, set_id, timestamp_begin, timestamp_end, name, description)
            .await
    }

    pub async fn update_slice_set(&self, id: i32, timestamp_begin: Option<DateTime<Utc>>, timestamp_end: Option<DateTime<Utc>>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Status>
    {
        slice::update_slice_set(&self, id, timestamp_begin, timestamp_end, name, description)
            .await
    }

    pub async fn delete_slice_set(&self, id: i32)
        -> Result<(), Status>
    {
        slice::delete_slice_set(&self, id)
            .await
    }

}
