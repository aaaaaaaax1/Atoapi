use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngCore;
#[cfg(not(test))]
use std::{fs, path::PathBuf};

#[cfg(not(test))]
use crate::config::app_config_dir;

#[cfg(not(test))]
const KEY_FILE: &str = "cache-key.dpapi";
const SECRET_PREFIX: &str = "dpapi:";

pub fn encrypt_secret(value: &str) -> Result<String> {
    let encrypted = protect_bytes(value.as_bytes())?;
    Ok(format!("{SECRET_PREFIX}{}", STANDARD.encode(encrypted)))
}

pub fn decrypt_secret(value: &str) -> Result<String> {
    let Some(encoded) = value.strip_prefix(SECRET_PREFIX) else {
        return Ok(value.to_string());
    };
    let encrypted = STANDARD.decode(encoded)?;
    let plain = unprotect_bytes(&encrypted)?;
    String::from_utf8(plain).context("decrypted secret was not valid UTF-8")
}

pub fn encrypt_cache_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let key = load_or_create_cache_key()?;
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| anyhow!("invalid AES key length"))?;
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), bytes)
        .map_err(|_| anyhow!("failed to encrypt cache payload"))?;
    let mut out = b"apx1".to_vec();
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

pub fn decrypt_cache_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() < 16 || &bytes[0..4] != b"apx1" {
        return Err(anyhow!("cache payload is not an encrypted Atoapi blob"));
    }
    let key = load_or_create_cache_key()?;
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| anyhow!("invalid AES key length"))?;
    cipher
        .decrypt(Nonce::from_slice(&bytes[4..16]), &bytes[16..])
        .map_err(|_| anyhow!("failed to decrypt cache payload"))
}

#[cfg(not(test))]
fn load_or_create_cache_key() -> Result<[u8; 32]> {
    let path = key_path()?;
    if path.exists() {
        let encrypted = fs::read(path)?;
        let plain = unprotect_bytes(&encrypted)?;
        let key: [u8; 32] = plain
            .try_into()
            .map_err(|_| anyhow!("cache key had unexpected length"))?;
        return Ok(key);
    }

    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, protect_bytes(&key)?)?;
    Ok(key)
}

#[cfg(test)]
fn load_or_create_cache_key() -> Result<[u8; 32]> {
    use std::sync::OnceLock;

    static TEST_CACHE_KEY: OnceLock<[u8; 32]> = OnceLock::new();
    Ok(*TEST_CACHE_KEY.get_or_init(|| {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        key
    }))
}

#[cfg(not(test))]
fn key_path() -> Result<PathBuf> {
    Ok(app_config_dir()?.join(KEY_FILE))
}

#[cfg(windows)]
fn protect_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    use std::ptr::null_mut;
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{CryptProtectData, CRYPT_INTEGER_BLOB},
    };

    let mut input = CRYPT_INTEGER_BLOB {
        cbData: bytes.len() as u32,
        pbData: bytes.as_ptr() as *mut u8,
    };
    let mut output = CRYPT_INTEGER_BLOB {
        cbData: 0,
        pbData: null_mut(),
    };

    let ok = unsafe {
        CryptProtectData(
            &mut input,
            null_mut(),
            null_mut(),
            null_mut(),
            null_mut(),
            0,
            &mut output,
        )
    };
    if ok == 0 {
        return Err(anyhow!("CryptProtectData failed"));
    }
    let protected = unsafe {
        let slice = std::slice::from_raw_parts(output.pbData, output.cbData as usize);
        let owned = slice.to_vec();
        LocalFree(output.pbData as _);
        owned
    };
    Ok(protected)
}

#[cfg(windows)]
fn unprotect_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    use std::ptr::null_mut;
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{CryptUnprotectData, CRYPT_INTEGER_BLOB},
    };

    let mut input = CRYPT_INTEGER_BLOB {
        cbData: bytes.len() as u32,
        pbData: bytes.as_ptr() as *mut u8,
    };
    let mut output = CRYPT_INTEGER_BLOB {
        cbData: 0,
        pbData: null_mut(),
    };

    let ok = unsafe {
        CryptUnprotectData(
            &mut input,
            null_mut(),
            null_mut(),
            null_mut(),
            null_mut(),
            0,
            &mut output,
        )
    };
    if ok == 0 {
        return Err(anyhow!("CryptUnprotectData failed"));
    }
    let plain = unsafe {
        let slice = std::slice::from_raw_parts(output.pbData, output.cbData as usize);
        let owned = slice.to_vec();
        LocalFree(output.pbData as _);
        owned
    };
    Ok(plain)
}

#[cfg(not(windows))]
fn protect_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    Ok(bytes.to_vec())
}

#[cfg(not(windows))]
fn unprotect_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    Ok(bytes.to_vec())
}
