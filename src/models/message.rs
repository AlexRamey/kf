use chrono::DateTime;

pub struct Message {
    pub topic: String,
    pub partition: u32,
    pub key: Option<String>,
    pub timestamp: DateTime,
    pub payload: Vec<u8>,
}
