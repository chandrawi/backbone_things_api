use chrono::Utc;
use serde::{Serialize, Deserialize};
use jsonwebtoken::{encode, decode, DecodingKey, EncodingKey, Header, Algorithm, Validation};

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenClaims {
    pub jti: i32,
    pub sub: String,
    pub iat: i64,
    pub exp: i64,
}

pub(crate) fn generate_token(jti: i32, sub: &str, duration: i32, key: &[u8]) -> Option<String>
{
    let iat = Utc::now().timestamp();
    let exp = iat + duration as i64;
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
