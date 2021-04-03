use kafcat::{
    configs::{KafkaConsumerConfig, SerdeFormat},
    error::KafcatError,
    interface::KafkaConsumer,
    rdkafka_impl::RdkafkaConsumer,
};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    time::{timeout_at, Instant},
};

use crate::modes::get_delay;

pub async fn run_async_consume_topic(config: KafkaConsumerConfig) -> Result<(), KafcatError> {
    let consumer = RdkafkaConsumer::from_config(config.clone()).await;
    consumer.set_offset_and_subscribe(config.offset).await?;
    let timeout = get_delay(config.exit_on_done);
    let mut stdout = BufWriter::new(tokio::io::stdout());
    let mut bytes_read = 0;
    let mut messages_count = 0;
    loop {
        match timeout_at(Instant::now() + timeout, consumer.recv()).await {
            Ok(Ok(msg)) => {
                log::trace!("Received message:\n{:#?}", msg);
                match config.format {
                    SerdeFormat::Text => {
                        bytes_read += stdout.write(&msg.key).await?;
                        bytes_read += stdout.write(config.key_delim.as_bytes()).await?;
                        bytes_read += stdout.write(&msg.payload).await?;
                        bytes_read += stdout.write(config.msg_delim.as_bytes()).await?;
                    }
                    SerdeFormat::Json => {
                        let x = serde_json::to_string(&msg)?;
                        bytes_read += stdout.write(x.as_bytes()).await?;
                        bytes_read += stdout.write(config.msg_delim.as_bytes()).await?;
                    }
                    SerdeFormat::Regex(r) => {
                        unimplemented!("Does not support {} yet", r);
                    }
                };
                messages_count += 1;
                let should_flush = {
                    match (config.msg_count_flush, config.msg_bytes_flush) {
                        (None, None) => false,
                        (None, Some(bytes_treshold)) => bytes_read >= bytes_treshold,
                        (Some(count_treshold), None) => messages_count >= count_treshold,
                        (Some(count_treshold), Some(bytes_treshold)) => {
                            bytes_read >= bytes_treshold || messages_count >= count_treshold
                        }
                    }
                };
                if should_flush {
                    stdout.flush().await?;
                    bytes_read = 0;
                    messages_count = 0;
                }
            }
            Ok(Err(err)) => return Err(err),
            Err(_err) => break,
        }
    }
    stdout.flush().await?;
    Ok(())
}
