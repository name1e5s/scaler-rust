use chrono::offset::Utc;

pub fn current_timestamp() -> u64 {
    Utc::now().timestamp() as u64
}

pub fn current_timestamp_ms() -> u64 {
    Utc::now().timestamp_millis() as u64
}
