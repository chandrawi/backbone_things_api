use tonic::{Request, Status, transport::Channel};
use uuid::Uuid;
use bbthings_grpc_server::proto::resource::config::config_service_client::ConfigServiceClient;
use bbthings_grpc_server::proto::resource::config::{ApiIdRequest, AccessRequest, ProcedureAcces, RoleAcces};
use crate::resource::Resource;

pub(crate) const API_ID_ERR: &str = "Api ID is not configured on the server";

pub(crate) async fn api_id(channel: &Channel) -> Result<Uuid, Status>
{
    let mut client = 
        ConfigServiceClient::new(channel.clone());
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
    let mut client = 
        ConfigServiceClient::new(resource.channel.clone());
    let request = Request::new(AccessRequest{});
    let response = client.procedure_access(request)
        .await?
        .into_inner();
    Ok(response.access)
}

pub(crate) async fn role_access(resource: &Resource) -> Result<Vec<RoleAcces>, Status>
{
    let mut client = 
        ConfigServiceClient::new(resource.channel.clone());
    let request = Request::new(AccessRequest{});
    let response = client.role_access(request)
        .await?
        .into_inner();
    Ok(response.access)
}
