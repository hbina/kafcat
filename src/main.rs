#![deny(unsafe_code)]

#[macro_use]
extern crate log;
pub mod modes;
use std::str::FromStr;

use modes::*;

use kafcat::{
    configs::{get_arg_matcher, KafkaConsumerConfig, KafkaProducerConfig},
    error::KafcatError,
    setup_logger,
};
use log::LevelFilter;

#[tokio::main]
async fn main() -> Result<(), KafcatError> {
    let args = std::env::args().collect::<Vec<String>>();

    let matches = get_arg_matcher().get_matches_from(args);

    let kafcat_log_env = std::env::var("KAFCAT_LOG").ok();
    let log_level = matches
        .value_of("log")
        .or_else(|| kafcat_log_env.as_deref())
        .map(|x| LevelFilter::from_str(x).expect("Cannot parse log level"))
        .unwrap_or(LevelFilter::Info);
    setup_logger(true, log_level);

    match matches.subcommand() {
        Some(("consume", matches)) => {
            run_async_consume_topic(KafkaConsumerConfig::from_matches(matches)).await?
        }
        Some(("produce", matches)) => {
            run_async_produce_topic(KafkaProducerConfig::from_matches(matches)).await?
        }
        Some(("copy", matches)) => {
            run_async_copy_topic(
                KafkaConsumerConfig::from_matches(matches),
                KafkaProducerConfig::from_matches(matches),
            )
            .await?
        }
        _ => unreachable!(),
    }
    Ok(())
}
