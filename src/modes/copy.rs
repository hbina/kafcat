use crate::modes::get_delay;
use kafcat::{
    configs::{KafkaConsumerConfig, KafkaProducerConfig},
    error::KafcatError,
    interface::{KafkaConsumer, KafkaProducer},
    rdkafka_impl::{RdkafkaConsumer, RdkafkaProducer},
};
use tokio::time::{timeout_at, Instant};

pub async fn run_async_copy_topic(
    consumer_config: KafkaConsumerConfig,
    producer_config: KafkaProducerConfig,
) -> Result<(), KafcatError> {
    let consumer = RdkafkaConsumer::from_config(consumer_config.clone()).await;
    consumer
        .set_offset_and_subscribe(consumer_config.offset)
        .await?;
    let timeout = get_delay(consumer_config.exit_on_done);
    let producer = RdkafkaProducer::from_config(producer_config.clone()).await;
    loop {
        match timeout_at(Instant::now() + timeout, consumer.recv()).await {
            Ok(Ok(msg)) => {
                producer.write_one(msg).await?;
            }
            Ok(Err(err)) => return Err(err),
            Err(_err) => break,
        }
    }
    Ok(())
}
