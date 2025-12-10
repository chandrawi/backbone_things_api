use sha2::Sha256;
use rsa::{RsaPrivateKey, RsaPublicKey, Oaep};
use pkcs8::{DecodePublicKey, EncodePublicKey};
use rand::thread_rng;
use tonic::Status;
pub use bbthings_database::utility::{generate_access_key, generate_token_string, hash_password, verify_password};

const ENCRYPT_ERR: &str = "encrypt message error";
const DECRYPT_ERR: &str = "decrypt password error";

pub(crate) fn generate_transport_keys() -> Result<(RsaPrivateKey, RsaPublicKey), rsa::Error>
{
    let mut rng = thread_rng();
    let bits = 1024;
    let priv_key = RsaPrivateKey::new(&mut rng, bits)?;
    let pub_key = RsaPublicKey::from(&priv_key);
    Ok((priv_key, pub_key))
}

pub(crate) fn export_public_key(pub_key: RsaPublicKey) -> Result<Vec<u8>, spki::Error>
{
    let pub_der = pub_key.to_public_key_der()?.to_vec();
    Ok(pub_der)
}

pub(crate) fn decrypt_message(ciphertext: &[u8], priv_key: RsaPrivateKey) -> Result<Vec<u8>, Status>
{
    let padding = Oaep::new_with_mgf_hash::<Sha256, Sha256>();
    priv_key.decrypt(padding, ciphertext)
        .map_err(|_| Status::internal(DECRYPT_ERR))
}

pub(crate) fn decrypt_message_string(ciphertext: &[u8], priv_key: RsaPrivateKey) -> Result<String, Status>
{
    String::from_utf8(decrypt_message(ciphertext, priv_key)?)
        .map_err(|_| Status::internal(DECRYPT_ERR))
}

pub fn encrypt_message(message: &[u8], pub_der: &[u8]) -> Result<Vec<u8>, Status>
{
    let pub_key = RsaPublicKey::from_public_key_der(pub_der)
        .map_err(|_| Status::internal(ENCRYPT_ERR))?;
    let padding = Oaep::new_with_mgf_hash::<Sha256, Sha256>();
    pub_key.encrypt(&mut thread_rng(), padding, message)
        .map_err(|_| Status::internal(ENCRYPT_ERR))
}

pub(crate) fn handle_error(e: sqlx::Error) -> tonic::Status {
    return match e {
        sqlx::Error::RowNotFound => tonic::Status::not_found(e.to_string()),
        sqlx::Error::InvalidArgument(message) => tonic::Status::invalid_argument(message),
        sqlx::Error::Database(db_err) => {
            match db_err.try_downcast::<sqlx::postgres::PgDatabaseError>() {
                Ok(pg_err) => {
                    let message = match pg_err.detail() {
                        Some(detail) => format!("{} {}", pg_err.message(), detail),
                        None => pg_err.message().to_string()
                    };
                    if pg_err.code() == "23505" {
                        tonic::Status::already_exists(message)
                    } else if pg_err.code() == "23501" || pg_err.code() == "23502" || pg_err.code() == "23503" {
                        tonic::Status::invalid_argument(message)
                    } else {
                        tonic::Status::internal(message)
                    }
                },
                Err(e) => tonic::Status::internal(e.message())
            }
        },
        _ => tonic::Status::unknown(e.to_string())
    }
}

pub fn hex_to_bytes(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| {
        s.get(i..i + 2)
            .and_then(|sub| u8::from_str_radix(sub, 16).ok())
    }).collect()
}
