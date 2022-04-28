use anyhow::{bail, Result};
use futures::TryStreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::OwnedMessage;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Message;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    Ok(())
}

pub trait MessageMatcher {
    fn is_match(&self, msg: OwnedMessage) -> Result<bool>;
}

#[derive(Clone)]
pub struct KeyMessageMatcher {
    pub key: String,
}

impl MessageMatcher for KeyMessageMatcher {
    fn is_match(&self, msg: OwnedMessage) -> Result<bool> {
        match get_key(&msg) {
            Ok(candidate_key) => Ok(self.key.eq(&candidate_key)),
            Err(_) => Ok(false),
        }
    }
}

fn get_key(msg: &OwnedMessage) -> Result<String> {
    let candidate_key = match msg.key() {
        Some(key) => key,
        None => bail!("Encountered invalid key!"),
    };
    match String::from_utf8(candidate_key.to_owned()) {
        Ok(key) => Ok(key),
        Err(_) => bail!("Encountered non-utf8 key!"),
    }
}

fn get_timestamp_millis(msg: &OwnedMessage) -> Result<i64> {
    match msg.timestamp().to_millis() {
        Some(millis) => Ok(millis),
        None => bail!("Encountered invalid timestamp!"),
    }
}

async fn search<T: MessageMatcher + Clone + Send + Sync + 'static>(matcher: &T) -> Result<()> {
    let (result_tx, mut result_rx) = mpsc::channel(32);
    let (shutdown_send, mut shutdown_recv) = mpsc::unbounded_channel();
    let consumer_group = Uuid::new_v4().to_string();
    let consumer_group_2 = consumer_group.clone();
    println!("CG: {}", consumer_group.clone());
    let topic = "quickstart".to_owned();
    let topic_2 = topic.clone();
    let matcher = matcher.clone();

    // Spawn consumer to search messages
    tokio::spawn(async move {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &consumer_group)
            .set("bootstrap.servers", "localhost:9092")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&[&topic])
            .expect("Can't subscribe to specified topic");
        let matcher = matcher.clone();
        let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
            let tx = result_tx.clone();
            let matcher = matcher.clone();
            let owned_message = borrowed_message.detach();
            async move {
                if matcher
                    .is_match(owned_message.clone())
                    .expect("Comparison failed!")
                {
                    tx.send(owned_message).await.expect("failed to send match");
                } else {
                    println!(
                        "NO MATCH (k: {} t: {} p: {} o: {})",
                        get_key(&owned_message).unwrap_or("'none'".to_owned()),
                        get_timestamp_millis(&owned_message).unwrap_or(-1),
                        owned_message.partition(),
                        owned_message.offset()
                    )
                }
                Ok(())
            }
        });

        stream_processor.await.expect("stream processing failed");
    });

    // Spawn consumer to monitor progress and send the shutdown signal
    tokio::spawn(async move {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &consumer_group_2)
            .set("bootstrap.servers", "localhost:9092")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Consumer creation failed");

        loop {
            sleep(Duration::from_millis(5000)).await;
            let timeout = Duration::from_millis(5000);
            let metadata = match consumer.fetch_metadata(Some(&topic_2), timeout) {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            let topics = metadata.topics();

            let mut num_partitions: i32 = 1;
            if topics.len() > 0 {
                num_partitions = topics[0].partitions().len() as i32;
            }

            let mut tpl = TopicPartitionList::new();
            for i in 0..num_partitions {
                tpl.add_partition(&topic_2, i);
            }

            let topic_map = match consumer.committed_offsets(tpl, timeout) {
                Ok(tp_list) => tp_list.to_topic_map(),
                Err(_) => continue,
            };

            println!("TOPIC MAP: {:?}", topic_map);
            if topic_map.len() == 0 {
                continue;
            }

            let mut is_done = true;
            let mut partition = 0;
            while let Some(Some(committed_offset)) = topic_map
                .get(&(topic_2.clone(), partition))
                .map(|co| co.to_raw())
            {
                println!("Committed offset: {}", committed_offset);
                match consumer.fetch_watermarks(&topic_2, partition, timeout) {
                    Ok((_, high)) => {
                        if high > committed_offset {
                            is_done = false;
                            break;
                        }
                    }
                    Err(_) => is_done = false,
                };
                partition += 1;
            }

            if is_done {
                println!("Finished consuming!");
                shutdown_send
                    .send(1)
                    .expect("unable to send shutdown signal");
            }
        }
    });

    tokio::spawn(async move {
        while let Some(owned_message) = result_rx.recv().await {
            println!(
                "MATCH (k: {} t: {} p: {} o: {})",
                get_key(&owned_message).unwrap_or("'none'".to_owned()),
                get_timestamp_millis(&owned_message).unwrap_or(-1),
                owned_message.partition(),
                owned_message.offset()
            );
        }
    });

    tokio::select! {
        _ = signal::ctrl_c() => { println!("CTRL-C"); }
        _ = shutdown_recv.recv() => { println!("Finished consuming"); }
    }

    Ok(())
}
