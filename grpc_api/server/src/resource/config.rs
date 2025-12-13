use tonic::{Request, Response, Status};
use crate::proto::resource::config::config_service_server::ConfigService;
use crate::proto::resource::config::{
    ApiIdRequest, ApiIdResponse, AccessRequest, ProcedureAccesResponse, RoleAccesResponse,
    ProcedureAcces, RoleAcces, 
};
use crate::common::validator::{AccessValidator, AccessSchema};
use crate::common::config::{API_ID, ACCESS_MAP};

const GET_ACCESS: &str = "get_access";

#[derive(Debug)]
pub struct ConfigServer {
    token_key: Vec<u8>,
    accesses: Vec<AccessSchema>
}

impl ConfigServer {
    pub fn new() -> Self {
        Self {
            token_key: Vec::new(),
            accesses: Vec::new()
        }
    }
    pub fn new_with_validator(token_key: &[u8], accesses: &[AccessSchema]) -> Self {
        const PROCEDURES: &[&str] = &[GET_ACCESS];
        Self {
            token_key: token_key.to_vec(),
            accesses: Self::construct_accesses(accesses, PROCEDURES)
        }
    }
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

    async fn procedure_access(&self, request: Request<AccessRequest>)
        ->  Result<Response<ProcedureAccesResponse>, Status>
    {
        self.validate(request.extensions(), GET_ACCESS)?;
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

    async fn role_access(&self, request: Request<AccessRequest>)
        ->  Result<Response<RoleAccesResponse>, Status>
    {
        self.validate(request.extensions(), GET_ACCESS)?;
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

impl AccessValidator for ConfigServer {

    fn token_key(&self) -> Vec<u8> {
        self.token_key.clone()
    }

    fn accesses(&self) -> Vec<AccessSchema> {
        self.accesses.clone()
    }

}
