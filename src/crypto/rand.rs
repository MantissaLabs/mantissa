use std::io;

/// Fill a buffer with cryptographically secure random bytes.
#[inline]
pub fn fill_random(buf: &mut [u8]) -> io::Result<()> {
    getrandom::getrandom(buf)?;
    Ok(())
}

/// Generate a 16-byte random nonce. (non-panicking)
#[inline]
pub fn try_nonce16() -> io::Result<[u8; 16]> {
    let mut n = [0u8; 16];
    fill_random(&mut n)?;
    Ok(n)
}

/// Generate a 16-byte random nonce. (panics on RNG failure)
#[inline]
pub fn nonce16() -> [u8; 16] {
    try_nonce16().expect("secure RNG unavailable")
}

/// Generate a random byte vector of length `len`. (non-panicking)
#[inline]
pub fn random_vec(len: usize) -> io::Result<Vec<u8>> {
    let mut v = vec![0u8; len];
    fill_random(&mut v)?;
    Ok(v)
}
