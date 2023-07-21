use crate::rpc;
pub use rpc::{Assignment, Result as IdleReason};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Meta {
    pub key: String,
    pub runtime: String,
    pub timeout_in_secs: u32,
    pub memory_in_mb: u64,
}

impl From<rpc::Meta> for Meta {
    fn from(v: rpc::Meta) -> Meta {
        Meta {
            key: v.key,
            runtime: v.runtime,
            timeout_in_secs: v.timeout_in_secs,
            memory_in_mb: v.memory_in_mb,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResourceConfig {
    pub memory_in_megabytes: u64,
}

impl From<rpc::ResourceConfig> for ResourceConfig {
    fn from(v: rpc::ResourceConfig) -> ResourceConfig {
        ResourceConfig {
            memory_in_megabytes: v.memory_in_megabytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Slot {
    pub instance_id: String,
    pub id: String,
    pub resource_config: ResourceConfig,
    pub create_time: u64,
    pub create_duration_in_ms: u64,
    pub init_time: u64,
    pub init_duration_in_ms: u64,
}
