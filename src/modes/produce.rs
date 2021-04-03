use kafcat::{
    configs::{KafkaProducerConfig, SerdeFormat},
    error::KafcatError,
    interface::KafkaProducer,
    message::KafkaMessage,
    rdkafka_impl::RdkafkaProducer,
};
use tokio::io::{AsyncBufReadExt, BufReader};

pub async fn run_async_produce_topic(config: KafkaProducerConfig) -> Result<(), KafcatError> {
    let producer = RdkafkaProducer::from_config(config.clone()).await;
    let reader = BufReader::new(tokio::io::stdin());
    let mut lines = reader.lines();
    let key_delim = config.key_delim;
    match config.format {
        SerdeFormat::Text => {
            while let Some(line) = lines.next_line().await? {
                if let Some(index) = line.find(&key_delim) {
                    let key = &line[..index];
                    let payload = &line[index + key_delim.len()..];
                    let msg = KafkaMessage {
                        key: key.to_owned().into_bytes(),
                        payload: payload.to_owned().into_bytes(),
                        timestamp: chrono::Utc::now().timestamp(),
                        headers: Default::default(),
                    };
                    producer.write_one(msg).await?;
                } else {
                    error!("Did not find key delimiter in line {}", line);
                }
            }
        }
        SerdeFormat::Json => {
            while let Some(line) = lines.next_line().await? {
                match serde_json::from_str(&line) {
                    Ok(msg) => producer.write_one(msg).await?,
                    Err(err) => {
                        error!("Error parsing json: {} {}", err, line)
                    }
                }
            }
        }
        SerdeFormat::Regex(r) => {
            unimplemented!("Does not support {} yet", r)
        }
    };

    Ok(())
}
