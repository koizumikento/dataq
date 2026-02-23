/// Deterministic FNV-1a hasher for execution fingerprint metadata.
#[derive(Debug, Clone)]
pub struct DeterministicHasher {
    state: u64,
}

impl DeterministicHasher {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    pub fn new() -> Self {
        Self {
            state: Self::OFFSET_BASIS,
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(Self::FNV_PRIME);
        }
    }

    pub fn update_len_prefixed(&mut self, bytes: &[u8]) {
        let len = bytes.len() as u64;
        self.update(&len.to_le_bytes());
        self.update(bytes);
    }

    pub fn finish_hex(&self) -> String {
        format!("{:016x}", self.state)
    }
}

impl Default for DeterministicHasher {
    fn default() -> Self {
        Self::new()
    }
}
