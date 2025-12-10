use tonic::{Request, Response, Status};
use crate::proto::resource::config::config_service_server::ConfigService;
use crate::proto::resource::config::{
    ApiIdRequest, ApiIdResponse, AccessRequest, ProcedureAccesResponse, RoleAccesResponse,
    ProcedureAcces, RoleAcces, 
};
use crate::common::config::{API_ID, ACCESS_MAP};

#[derive(Debug)]
pub struct ConfigServer {
}

#[tonic::async_trait]
impl ConfigService for ConfigServer {

    async fn api_id(&self, _: Request<ApiIdRequest>) 
        ->  Result<Response<ApiIdResponse>, Status>
    {
        let api_id = match API_ID.get() {
            Some(value) => value.as_bytes().to_vec(),
            None => Vec::new()
        };
        Ok(Response::new(ApiIdResponse { api_id }))
    }

    async fn procedure_access(&self, _: Request<AccessRequest>)
        ->  Result<Response<ProcedureAccesResponse>, Status>
    {
        let access = match ACCESS_MAP.get() {
            Some(value) => value.into_iter().map(|a| {
                    ProcedureAcces {
                        procedure: a.procedure.clone(),
                        roles: a.roles.clone()
                    }
                }).collect(),
            None => Vec::new()
        };
        Ok(Response::new(ProcedureAccesResponse { access }))
    }

    async fn role_access(&self, _: Request<AccessRequest>)
        ->  Result<Response<RoleAccesResponse>, Status>
    {
        let access = match ACCESS_MAP.get() {
            Some(value) => {
                let mut pairs: Vec<(String, String)> = Vec::new();
                for access in value {
                    for role in access.roles.clone() {
                        pairs.push((role, access.procedure.clone()));
                    }
                }
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                let mut result = Vec::new();
                let mut iter = pairs.into_iter();
                if let Some((mut current_role, procedure)) = iter.next() {
                    let mut procedures = vec![procedure];
                    for (role, procedure) in iter {
                        if role == current_role {
                            procedures.push(procedure);
                        } else {
                            result.push(RoleAcces {
                                role: current_role,
                                procedures,
                            });
                            current_role = role;
                            procedures = vec![procedure];
                        }
                    }
                    result.push(RoleAcces {
                        role: current_role,
                        procedures,
                    });
                }
                result
            },
            None => Vec::new()
        };
        Ok(Response::new(RoleAccesResponse { access }))
    }

}
