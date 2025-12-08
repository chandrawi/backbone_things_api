use tonic::{Status, Extensions};
use uuid::Uuid;
use chrono::Utc;
use super::token::decode_token;
use super::config::{ROOT_ID, ROOT_NAME, ROOT_DATA};
use bbthings_database::Auth;
use crate::proto::auth::auth::ProcedureMap;

const EXT_NOT_FOUND: &str = "Extension not found";
const TOKEN_EXPIRED: &str = "Token is broken or expired";
const PROC_NOT_FOUND: &str = "Procedure access not found";
const USER_UNREGISTERED: &str = "User is not logged in or not registered";
const ACCESS_RIGHT_ERR: &str = "doesn't has access rights";

#[derive(Debug, Clone)]
pub struct AccessSchema {
    pub procedure: String,
    pub roles: Vec<String>
}

impl From<ProcedureMap> for AccessSchema {
    fn from(value: ProcedureMap) -> Self {
        Self { procedure: value.procedure, roles: value.roles }
    }
}

pub(crate) trait AccessValidator {

    fn token_key(&self) -> Vec<u8>;

    fn accesses(&self) -> Vec<AccessSchema>;

    fn construct_accesses(accesses: &[AccessSchema], procedures: &[&str]) -> Vec<AccessSchema>
    {
        procedures.into_iter().map(|&s| AccessSchema {
            procedure: s.to_owned(),
            roles: accesses.iter()
                .filter(|&a| a.procedure == s)
                .map(|a| a.roles.clone())
                .next()
                .unwrap_or_default()
        })
        .collect()
    }

    fn validate(&self, extension: &Extensions, procedure: &str) -> Result<(), Status>
    {
        // return ok if service doesn't configured to use validation
        if self.accesses().len() == 0 {
            return Ok(());
        }
        // try to decode token from request extension using api accees key or root key and then get token claims
        let token = extension.get::<String>()
            .ok_or(Status::unauthenticated(EXT_NOT_FOUND))?;
        let mut decoded = decode_token(token, &self.token_key(), true);
        if decoded.is_none() {
            let root = ROOT_DATA.get().map(|x| x.to_owned()).unwrap_or_default();
            decoded = decode_token(token, &root.access_key, true);
        }
        let claims = match decoded {
            Some(value) => value,
            None => return Err(Status::unauthenticated(TOKEN_EXPIRED))
        };
        // pass checking for root role
        if &claims.sub == ROOT_NAME {
            return Ok(())
        }
        // check if the role in token claims has accsess rights to the procedure
        let access = self.accesses()
            .into_iter()
            .filter(|a| a.procedure == procedure)
            .next()
            .ok_or(Status::internal(PROC_NOT_FOUND))?;
        let role = access.roles
            .into_iter()
            .filter(|r| *r == claims.sub)
            .next();
        match role {
            Some(_) => Ok(()),
            None => Err(Status::unauthenticated(
                format!("Role {} {}", claims.sub, ACCESS_RIGHT_ERR)
            ))
        }
    }

}

pub enum ValidatorKind {
    User(Uuid),
    Root
}

pub(crate) trait AuthValidator {

    fn validator_flag(&self) -> bool;

    fn auth_db(&self) -> &Auth;

    async fn validate(&self, extension: &Extensions, kind: ValidatorKind) -> Result<(), Status>
    {
        // return ok if service doesn't configured to use validation
        if !self.validator_flag() {
            return Ok(());
        }
        // get user id from extension and check if token is expired
        let token = extension.get::<String>()
            .ok_or(Status::unauthenticated(EXT_NOT_FOUND))?;
        let result = self.auth_db().list_auth_token(token).await;
        let user_id = match result {
            Ok(value) => match value.into_iter().next() {
                Some(v) => {
                    if v.expired < Utc::now() { return Err(Status::unauthenticated(TOKEN_EXPIRED)) }
                    v.user_id
                },
                None => return Err(Status::unauthenticated(USER_UNREGISTERED))
            },
            Err(_) => return Err(Status::unauthenticated(USER_UNREGISTERED))
        };
        // check input user id or root user
        if let ValidatorKind::User(id) = kind {
            if id == user_id {
                return Ok(());
            }
        }
        if user_id == ROOT_ID {
            Ok(())
        } else {
            Err(Status::unauthenticated(format!("User {} {}", user_id, ACCESS_RIGHT_ERR)))
        }
    }

}
