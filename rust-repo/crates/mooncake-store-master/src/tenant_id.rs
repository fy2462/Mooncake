use serde::{Deserialize, Deserializer, Serialize};
use std::borrow::Borrow;
use std::fmt;
use thiserror::Error;

pub const DEFAULT_TENANT: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct TenantId(String);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TenantIdError {
    #[error("tenant id uses the reserved '_' prefix")]
    ReservedPrefix,
    #[error("tenant id contains invalid byte 0x{0:02x}")]
    InvalidByte(u8),
}

impl TenantId {
    pub fn new(raw: String) -> Result<Self, TenantIdError> {
        let value = if raw.is_empty() {
            DEFAULT_TENANT.to_owned()
        } else {
            raw
        };
        if value.starts_with('_') {
            return Err(TenantIdError::ReservedPrefix);
        }
        if let Some(byte) = value.bytes().find(|byte| *byte < 0x20 || *byte == 0x7f) {
            return Err(TenantIdError::InvalidByte(byte));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn is_default(&self) -> bool {
        self.0 == DEFAULT_TENANT
    }

    pub fn make_scoped_key(&self, local_key: &str) -> String {
        let mut key = String::with_capacity(self.0.len() + 1 + local_key.len());
        key.push_str(&self.0);
        key.push('\0');
        key.push_str(local_key);
        key
    }

    pub fn parse_scoped_key(scoped: &str) -> Result<(Self, String), TenantIdError> {
        match scoped.split_once('\0') {
            Some((tenant, key)) => Ok((Self::new(tenant.to_owned())?, key.to_owned())),
            None => Ok((Self::default(), scoped.to_owned())),
        }
    }
}

impl Default for TenantId {
    fn default() -> Self {
        Self(DEFAULT_TENANT.to_owned())
    }
}

impl AsRef<str> for TenantId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for TenantId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TenantId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::{TenantId, TenantIdError};
    use std::collections::{BTreeSet, HashMap, HashSet};

    #[test]
    fn cpp_parity_tenant_id_test_cpp_tenantidtest_supportsorderedandhashedkeys_8e2784aa() {
        let tenant_a = TenantId::new("tenant-a".into()).unwrap();
        let tenant_b = TenantId::new("tenant-b".into()).unwrap();

        assert!(tenant_a < tenant_b);
        let mut tenants = HashMap::new();
        tenants.insert(tenant_a.clone(), 1);
        assert_eq!(tenants[&tenant_a], 1);
    }

    #[test]
    fn normalizes_and_validates_canonical_values() {
        assert_eq!(TenantId::new(String::new()).unwrap(), TenantId::default());
        assert!(TenantId::new("tenant:模型".into()).is_ok());
        assert!(matches!(
            TenantId::new("_reserved".into()),
            Err(TenantIdError::ReservedPrefix)
        ));
        for value in ["bad\nname", "bad\0name", "bad\u{7f}"] {
            assert!(matches!(
                TenantId::new(value.into()),
                Err(TenantIdError::InvalidByte(_))
            ));
        }
    }

    #[test]
    fn supports_ordering_and_hashing() {
        let default = TenantId::default();
        let tenant = TenantId::new("tenant:one".into()).unwrap();
        assert!(default < tenant);

        let mut ordered = BTreeSet::new();
        ordered.insert(tenant.clone());
        ordered.insert(default.clone());
        assert_eq!(
            ordered.into_iter().collect::<Vec<_>>(),
            vec![default.clone(), tenant.clone()]
        );

        let mut hashed = HashSet::new();
        hashed.insert(tenant.clone());
        assert!(hashed.contains(&tenant));
    }

    #[test]
    fn scoped_key_preserves_wire_format_and_local_nuls() {
        let tenant = TenantId::new("tenant:one".into()).unwrap();
        let local = "part1\0part2";
        let scoped = tenant.make_scoped_key(local);
        assert_eq!(scoped.as_bytes(), b"tenant:one\0part1\0part2");
        assert_eq!(
            TenantId::parse_scoped_key(&scoped).unwrap(),
            (tenant, local.into())
        );
        assert_eq!(
            TenantId::parse_scoped_key("legacy").unwrap(),
            (TenantId::default(), "legacy".into())
        );
    }

    #[test]
    fn serde_rejects_invalid_tenant_values() {
        assert_eq!(
            serde_json::to_string(&TenantId::default()).unwrap(),
            "\"default\""
        );
        assert!(serde_json::from_str::<TenantId>("\"_reserved\"").is_err());
    }
}
