use crate::adapters::App;
use crate::models::Command;
use clap_v3::{App, Arg};

pub fn 

    let matches = App::new("kf")
        .version("1.0")
        .author("Alex Ramey <alexander.ramey@spacex.corp>")
        .about(
            "Search Kafka for messages by key or rfc3339 ('1996-12-19T16:39:57-08:00') datetime.",
        )
        .arg(
            Arg::with_name("key")
                .help("Message key to search for")
                .short('k')
                .long("key")
                .takes_value(true)
                .required(true)
                .conflicts_with("datetime"),
        )
        .arg(
            Arg::with_name("datetime")
                .help("Message time to search for")
                .short('d')
                .long("datetime")
                .takes_value(true)
                .required_unless("key"),
        )
        .get_matches();

    if let Some(key) = matches.value_of("key") {
        println!("Searching by key {}!", key);
        let matcher = KeyMessageMatcher {
            key: key.to_owned(),
        };
        search(&matcher).await.expect("search failed!");
    } else {
        let datetime = matches.value_of("datetime").unwrap();
        let datetime = match DateTime::parse_from_rfc3339(datetime) {
            Ok(datetime) => datetime,
            Err(_) => bail!(
                "Provided datetime '{}' is not rfc3339 ('1996-12-19T16:39:57-08:00') format",
                datetime
            ),
        };
        println!("Searching by time: {}!", datetime)
    }