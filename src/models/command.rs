use chrono::DateTime;

pub enum Command {
    Search(SearchFilter),
}

pub enum SearchFilter {
    Key(String),
    Date(DateTime),
}
