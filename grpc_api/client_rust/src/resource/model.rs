use tonic::{Request, Status};
use uuid::Uuid;
use bbthings_database::common::type_value::{DataType, DataValue};
use bbthings_grpc_proto::resource::model::model_service_client::ModelServiceClient;
use bbthings_grpc_proto::resource::model::{
    ModelSchema, ModelId, ModelIds, ModelName, ModelCategory, ModelOption, TypeId, ModelUpdate, 
    ConfigSchema, ConfigId, ConfigUpdate, TagSchema, TagId, TagUpdate
};
use crate::resource::Resource;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

const MODEL_NOT_FOUND: &str = "requested model not found";
const CONF_NOT_FOUND: &str = "requested config not found";

pub(crate) async fn read_model(resource: &Resource, id: Uuid)
    -> Result<ModelSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ModelId {
        id: id.as_bytes().to_vec()
    });
    let response = client.read_model(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(MODEL_NOT_FOUND))?)
}

pub(crate) async fn list_model_by_ids(resource: &Resource, ids: &[Uuid])
    -> Result<Vec<ModelSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ModelIds {
        ids: ids.into_iter().map(|&id| id.as_bytes().to_vec()).collect()
    });
    let response = client.list_model_by_ids(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_model_by_type(resource: &Resource, type_id: Uuid)
    -> Result<Vec<ModelSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(TypeId {
        id: type_id.as_bytes().to_vec()
    });
    let response = client.list_model_by_type(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_model_by_name(resource: &Resource, name: &str)
    -> Result<Vec<ModelSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ModelName {
        name: name.to_owned()
    });
    let response = client.list_model_by_name(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_model_by_category(resource: &Resource, category: &str)
    -> Result<Vec<ModelSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ModelCategory {
        category: category.to_owned()
    });
    let response = client.list_model_by_category(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn list_model_option(resource: &Resource, type_id: Option<Uuid>, name: Option<&str>, category: Option<&str>)
    -> Result<Vec<ModelSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ModelOption {
        type_id: type_id.map(|id| id.as_bytes().to_vec()),
        name: name.map(|s| s.to_owned()),
        category: category.map(|s| s.to_owned())
    });
    let response = client.list_model_option(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_model(resource: &Resource, id: Uuid, data_type: &[DataType], category: &str, name: &str, description: Option<&str>)
    -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ModelSchema {
        id: id.as_bytes().to_vec(),
        category: category.to_owned(),
        name: name.to_owned(),
        description: description.unwrap_or("").to_owned(),
        data_type: data_type.into_iter().map(|ty| u32::from(ty.to_owned())).collect::<Vec<u32>>().to_owned(),
        tags: Vec::new(),
        configs: Vec::new()
    });
    let response = client.create_model(request)
        .await?
        .into_inner();
    Ok(Uuid::from_slice(&response.id).unwrap_or_default())
}

pub(crate) async fn update_model(resource: &Resource, id: Uuid, data_type: Option<&[DataType]>, category: Option<&str>, name: Option<&str>, description: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ModelUpdate {
        id: id.as_bytes().to_vec(),
        category: category.map(|s| s.to_owned()),
        name: name.map(|s| s.to_owned()),
        description: description.map(|s| s.to_owned()),
        data_type: data_type.map(|ty| {
            ty.into_iter()
                .map(|t| u32::from(t.to_owned()))
                .collect::<Vec<u32>>()
        }).unwrap_or_default(),
        data_type_flag: data_type.is_some()
    });
    client.update_model(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_model(resource: &Resource, id: Uuid)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ModelId {
        id: id.as_bytes().to_vec()
    });
    client.delete_model(request)
        .await?;
    Ok(())
}

pub(crate) async fn read_model_config(resource: &Resource, id: i32)
    -> Result<ConfigSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ConfigId {
        id
    });
    let response = client.read_model_config(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(CONF_NOT_FOUND))?)
}

pub(crate) async fn list_model_config_by_model(resource: &Resource, model_id: Uuid)
    -> Result<Vec<ConfigSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ModelId {
        id: model_id.as_bytes().to_vec()
    });
    let response = client.list_model_config(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_model_config(resource: &Resource, model_id: Uuid, index: i32, name: &str, value: DataValue, category: &str)
    -> Result<i32, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ConfigSchema {
        id: 0,
        model_id: model_id.as_bytes().to_vec(),
        index,
        name: name.to_owned(),
        config_bytes: value.to_bytes(),
        config_type: value.get_type().into(),
        category: category.to_owned(),
    });
    let response = client.create_model_config(request)
        .await?
        .into_inner();
    Ok(response.id)
}

pub(crate) async fn update_model_config(resource: &Resource, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ConfigUpdate {
        id,
        name: name.map(|s| s.to_owned()),
        config_bytes: value.clone().map(|s| s.to_bytes()),
        config_type: value.map(|s| s.get_type().into()),
        category: category.map(|s| s.to_owned())
    });
    client.update_model_config(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_model_config(resource: &Resource, id: i32)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ConfigId {
        id
    });
    client.delete_model_config(request)
        .await?;
    Ok(())
}

pub(crate) async fn read_tag(resource: &Resource, model_id: Uuid, tag: i16)
    -> Result<TagSchema, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(TagId {
        model_id: model_id.as_bytes().to_vec(),
        tag: tag as i32
    });
    let response = client.read_tag(request)
        .await?
        .into_inner();
    Ok(response.result.ok_or(Status::not_found(CONF_NOT_FOUND))?)
}

pub(crate) async fn list_tag_by_model(resource: &Resource, model_id: Uuid)
    -> Result<Vec<TagSchema>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(ModelId {
        id: model_id.as_bytes().to_vec()
    });
    let response = client.list_tag_by_model(request)
        .await?
        .into_inner();
    Ok(response.results)
}

pub(crate) async fn create_tag(resource: &Resource, model_id: Uuid, tag: i16, name: &str, members: &[i16])
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(TagSchema {
        model_id: model_id.as_bytes().to_vec(),
        tag: tag as i32,
        name: name.to_owned(),
        members: members.into_iter().map(|&t| t as i32).collect()
    });
    client.create_tag(request)
        .await?
        .into_inner();
    Ok(())
}

pub(crate) async fn update_tag(resource: &Resource, model_id: Uuid, tag: i16, name: Option<&str>, members: Option<&[i16]>)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(TagUpdate {
        model_id: model_id.as_bytes().to_vec(),
        tag: tag as i32,
        name: name.map(|s| s.to_owned()),
        members: members.clone().unwrap_or_default().into_iter().map(|&t| t as i32).collect(),
        members_flag: members.is_some()
    });
    client.update_tag(request)
        .await?;
    Ok(())
}

pub(crate) async fn delete_tag(resource: &Resource, model_id: Uuid, tag: i16)
    -> Result<(), Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ModelServiceClient::with_interceptor(resource.channel.to_owned(), interceptor);
    let request = Request::new(TagId {
        model_id: model_id.as_bytes().to_vec(),
        tag: tag as i32
    });
    client.delete_tag(request)
        .await?;
    Ok(())
}
