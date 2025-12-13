use tonic::{Request, Status};
use uuid::Uuid;
use bbthings_grpc_server::proto::resource::config::config_service_client::ConfigServiceClient;
use bbthings_grpc_server::proto::resource::config::{ApiIdRequest, AccessRequest, ProcedureAcces, RoleAcces};
use crate::resource::Resource;
use bbthings_grpc_server::common::interceptor::TokenInterceptor;

pub(crate) const API_ID_ERR: &str = "Api ID is not configured on the server";

pub(crate) async fn api_id(resource: &Resource) -> Result<Uuid, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ConfigServiceClient::with_interceptor(resource.channel.clone(), interceptor);
    let request = Request::new(ApiIdRequest{});
    let response = client.api_id(request)
        .await?
        .into_inner();
    let api_id = Uuid::from_slice(&response.api_id)
        .map_err(|_| Status::not_found(API_ID_ERR))?;
    Ok(api_id)
}

pub(crate) async fn procedure_access(resource: &Resource) -> Result<Vec<ProcedureAcces>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ConfigServiceClient::with_interceptor(resource.channel.clone(), interceptor);
    let request = Request::new(AccessRequest{});
    let response = client.procedure_access(request)
        .await?
        .into_inner();
    Ok(response.access)
}

pub(crate) async fn role_access(resource: &Resource) -> Result<Vec<RoleAcces>, Status>
{
    let interceptor = TokenInterceptor(resource.access_token.clone());
    let mut client = 
        ConfigServiceClient::with_interceptor(resource.channel.clone(), interceptor);
    let request = Request::new(AccessRequest{});
    let response = client.role_access(request)
        .await?
        .into_inner();
    Ok(response.access)
}
