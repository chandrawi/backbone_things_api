use sqlx::Error;
use rand::{thread_rng, Rng};
use argon2::{Argon2, PasswordHasher, PasswordVerifier, PasswordHash, password_hash::SaltString};

pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error>
{
    let argon2 = Argon2::default();
    let salt = SaltString::generate(&mut thread_rng());
    let password_hash = argon2.hash_password(password.as_bytes(), &salt)?;
    Ok(password_hash.to_string())
}

pub fn verify_password(password: &str, password_hash: &str) -> Result<(), argon2::password_hash::Error>
{
    let parsed_hash = PasswordHash::new(password_hash).unwrap();
    Argon2::default().verify_password(password.as_bytes(), &parsed_hash)
}

pub(crate) fn verify_hash_format(password_hash: &str) -> Result<(), Error>
{
    match PasswordHash::new(password_hash) {
        Ok(_) => Ok(()),
        Err(_) => Err(Error::InvalidArgument(String::from("Invalid password hash format")))
    }
}

pub fn generate_access_key() -> Vec<u8>
{
    (0..32).map(|_| thread_rng().gen_range(0..255)).collect()
}

pub fn generate_token_string() -> String
{
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";
    (0..32).map(|_| CHARSET[thread_rng().gen_range(0..64)] as char).collect()
}
