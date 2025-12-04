use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use jsonwebtoken::{encode, decode, DecodingKey, EncodingKey, Header, Algorithm, Validation};

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenClaims {
    pub jti: i32,
    pub sub: String,
    pub iat: u64,
    pub exp: u64,
}

pub(crate) fn generate_token(jti: i32, sub: &str, duration: i32, key: &[u8]) -> Option<String>
{
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let iat = now.as_secs();
    let exp = iat + duration as u64;
    let claims = TokenClaims {
        jti,
        sub: sub.to_owned(),
        iat,
        exp
    };
    let header = Header::new(Algorithm::HS256);
    let encoding_key = EncodingKey::from_secret(key);
    encode(&header, &claims, &encoding_key).ok()
}

pub(crate) fn decode_token(token: &str, key: &[u8], exp_flag: bool) -> Option<TokenClaims>
{
    let decoding_key = DecodingKey::from_secret(key);
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_required_spec_claims(&["exp"]);
    validation.validate_exp = exp_flag;
    let token_data = decode::<TokenClaims>(token, &decoding_key, &validation).ok();
    token_data.map(|value| value.claims)
}
