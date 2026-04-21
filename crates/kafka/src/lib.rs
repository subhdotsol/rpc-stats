pub use rdkafka::producer::{FutureProducer, FutureRecord};
pub use rdkafka::consumer::{StreamConsumer};
use rdkafka::ClientConfig;
use std::time::Duration;

pub mod topics;

pub fn create_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error")
}

pub fn create_consumer(brokers: &str, group_id: &str) -> anyhow::Result<StreamConsumer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?)
}

pub async fn produce_message(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    payload: &str,
) -> anyhow::Result<()> {

    producer
        .send(
            FutureRecord::to(topic).key(key).payload(payload),
            Duration::from_secs(0),
        )
        .await
        .map_err(|(e, _)| anyhow::anyhow!("Kafka send error: {:?}", e))?;
    Ok(())
}
