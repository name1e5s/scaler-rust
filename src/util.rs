use chrono::offset::Utc;

pub fn current_timestamp() -> u64 {
    Utc::now().timestamp() as u64
}
