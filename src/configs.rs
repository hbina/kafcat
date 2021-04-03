use clap::crate_version;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use regex::Regex;
use std::str::FromStr;

const GROUP_ID_DEFAULT: &str = "kafcat";
const BROKERS_DEFAULT: &str = "localhost:9092";
const MSG_DELIMITER_DEFAULT: &str = "\n";
const KEY_DELIMITER_DEFAULT: &str = ":";
const OFFSET_DEFAULT: &str = "beginning";
const FORMAT_DEFAULT: &str = "text";

pub fn group_id() -> Arg<'static> {
    Arg::new("group-id")
        .short('G')
        .long("group-id")
        .about("Consumer group id. (Kafka >=0.9 balanced consumer groups)")
        .default_value(GROUP_ID_DEFAULT)
}

pub fn offset() -> Arg<'static> {
    Arg::new("offset")
        .short('o')
        .takes_value(true)
        .default_value(r#"beginning"#)
        .long_about(
            r#"Offset to start consuming from:
                     beginning | end | stored |
                     <value>  (absolute offset) |
                     -<value> (relative offset from end)
                     s@<value> (timestamp in ms to start at)
                     e@<value> (timestamp in ms to stop at (not included))"#,
        )
}

pub fn topic() -> Arg<'static> {
    Arg::new("topic")
        .short('t')
        .long("topic")
        .about("Topic")
        .takes_value(true)
}

pub fn brokers() -> Arg<'static> {
    Arg::new("brokers")
        .short('b')
        .long("brokers")
        .about("Broker list in kafka format")
        .default_value(BROKERS_DEFAULT)
}

pub fn partition() -> Arg<'static> {
    Arg::new("partition")
        .short('p')
        .long("partition")
        .about("Partition")
        .takes_value(true)
}

pub fn exit() -> Arg<'static> {
    Arg::new("exit")
        .short('e')
        .long("exit")
        .about("Exit successfully when last message received")
}

pub fn format() -> Arg<'static> {
    Arg::new("format")
        .short('s')
        .long("format")
        .about("Serialize/Deserialize format")
        .default_value(FORMAT_DEFAULT)
}

pub fn flush_count() -> Arg<'static> {
    Arg::new("flush-count")
        .long("flush-count")
        .about("Number of messages to receive before flushing stdout")
        .takes_value(true)
}

pub fn flush_bytes() -> Arg<'static> {
    Arg::new("flush-bytes")
        .long("flush-bytes")
        .about("Size of messages in bytes accumulated before flushing to stdout")
        .takes_value(true)
}

pub fn msg_delimiter() -> Arg<'static> {
    Arg::new("msg-delimiter")
        .short('D')
        .about("Delimiter to split input into messages(currently only supports '\\n')")
        .default_value(MSG_DELIMITER_DEFAULT)
}

pub fn key_delimiter() -> Arg<'static> {
    Arg::new("key-delimiter")
        .short('K')
        .about("Delimiter to split input key and message")
        .default_value(KEY_DELIMITER_DEFAULT)
}

pub fn consume_subcommand() -> App<'static> {
    App::new("consume").short_flag('C').args(vec![
        brokers(),
        group_id(),
        topic().required(true),
        partition(),
        offset(),
        exit(),
        msg_delimiter(),
        key_delimiter(),
        format(),
        flush_count(),
        flush_bytes(),
    ])
}

pub fn produce_subcommand() -> App<'static> {
    App::new("produce").short_flag('P').args(vec![
        brokers(),
        group_id(),
        topic().required(true),
        partition(),
        msg_delimiter(),
        key_delimiter(),
        format(),
    ])
}

pub fn copy_subcommand() -> App<'static> {
    // this is not meant to be used directly only for help message
    App::new("copy")
        .setting(AppSettings::AllowLeadingHyphen)
        .alias("--cp")
        .arg(Arg::new("from").multiple(true).required(true))
        .arg(Arg::new("to").multiple(true).last(true).required(true))
        .about("Copy mode accepts two parts of arguments <from> and <to>, the two parts are separated by [--]. <from> is the exact as Consumer mode, and <to> is the exact as Producer mode.")
}

pub fn get_arg_matcher() -> App<'static> {
    App::new("kafcat")
        .version(crate_version!())
        .author("Jiangkun Qiu <qiujiangkun@foxmail.com>")
        .about("cat but kafka")
        .subcommands(vec![
            consume_subcommand(),
            produce_subcommand(),
            copy_subcommand(),
        ])
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::new("log")
                .long("log")
                .about("Configure level of logging")
                .takes_value(true)
                // See https://docs.rs/env_logger/0.8.3/env_logger/#enabling-logging
                .possible_values(&["error", "warn", "info", "debug", "trace"]),
        )
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum KafkaOffset {
    Beginning,
    End,
    Stored,
    Offset(isize),
    OffsetInterval(i64, i64),
    TimeInterval(i64, i64),
}

impl Default for KafkaOffset {
    fn default() -> Self {
        Self::Beginning
    }
}

impl FromStr for KafkaOffset {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, String> {
        Ok(match value.to_ascii_lowercase().as_str() {
            "beginning" => KafkaOffset::Beginning,
            "end" => KafkaOffset::End,
            "stored" => KafkaOffset::Stored,
            _ => {
                if let Ok(val) = value.parse() {
                    KafkaOffset::Offset(val)
                } else if let Some(x) = Regex::new(r"s@(\d+)e@(\d+)$").unwrap().captures(value) {
                    let b = x.get(1).unwrap().as_str();
                    let e = x.get(2).unwrap().as_str();
                    KafkaOffset::TimeInterval(b.parse().unwrap(), e.parse().unwrap())
                } else {
                    return Err(format!("Cannot parse {} as offset", value));
                }
            }
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum SerdeFormat {
    Text,
    Json,
    Regex(String),
}

impl FromStr for SerdeFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, String> {
        Ok(match value {
            "text" => SerdeFormat::Text,
            "json" => SerdeFormat::Json,
            _ => SerdeFormat::Regex(value.to_owned()),
        })
    }
}
#[rustfmt::skip]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct KafkaConsumerConfig {
    pub brokers:            String,
    pub group_id:           String,
    pub offset:             KafkaOffset,
    pub partition:          Option<i32>,
    pub topic:              String,
    pub exit_on_done:       bool,
    pub msg_delim:          String,
    pub key_delim:          String,
    pub format:             SerdeFormat,
    pub msg_count_flush:    Option<usize>,
    pub msg_bytes_flush:    Option<usize>
}

impl KafkaConsumerConfig {
    pub fn from_args(args: &[&str]) -> Self {
        let matches = get_arg_matcher().get_matches_from(args);
        KafkaConsumerConfig::from_matches(&matches)
    }

    pub fn from_matches(matches: &ArgMatches) -> KafkaConsumerConfig {
        let brokers = matches
            .value_of("brokers")
            .expect("Must specify brokers")
            .to_owned();
        let group_id = matches.value_of("group-id").unwrap_or("kafcat").to_owned();
        let offset = matches
            .value_of("offset")
            .map(|x| x.parse().expect("Cannot parse offset"))
            .unwrap_or(KafkaOffset::Beginning);
        let partition = matches
            .value_of("partition")
            .map(|x| x.parse().expect("Cannot parse partition"));
        let exit = matches.occurrences_of("exit") > 0;
        let topic = matches
            .value_of("topic")
            .expect("Must specify topic")
            .to_owned();
        let format = matches.value_of("format").expect("Must specify format");
        let msg_delim = matches.value_of("msg-delimiter").unwrap().to_owned();
        let key_delim = matches.value_of("key-delimiter").unwrap().to_owned();
        let msg_count_flush = matches.value_of("flush-count").map(|x| x.parse().unwrap());
        let msg_bytes_flush = matches.value_of("flush-bytes").map(|x| x.parse().unwrap());
        KafkaConsumerConfig {
            brokers,
            group_id,
            offset,
            partition,
            topic,
            format: SerdeFormat::from_str(format).unwrap(),
            exit_on_done: exit,
            msg_delim,
            key_delim,
            msg_count_flush,
            msg_bytes_flush,
        }
    }
}

impl Default for KafkaConsumerConfig {
    #[rustfmt::skip]
    fn default() -> Self {
        KafkaConsumerConfig {
            brokers:            BROKERS_DEFAULT.to_string(),
            group_id:           GROUP_ID_DEFAULT.to_string(),
            offset:             KafkaOffset::from_str(OFFSET_DEFAULT).unwrap(),
            partition:          None,
            topic:              "".to_string(),
            format:             SerdeFormat::from_str(FORMAT_DEFAULT).unwrap(),
            exit_on_done:       false,
            msg_delim:          MSG_DELIMITER_DEFAULT.to_string(),
            key_delim:          KEY_DELIMITER_DEFAULT.to_string(),
            msg_count_flush:    None,
            msg_bytes_flush:    None
        }
    }
}

#[rustfmt::skip]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct KafkaProducerConfig {
    pub brokers:   String,
    pub group_id:  String,
    pub partition: Option<i32>,
    pub topic:     String,
    pub msg_delim: String,
    pub key_delim: String,
    pub format:    SerdeFormat,
}

impl KafkaProducerConfig {
    pub fn from_args(args: &[&str]) -> Self {
        let matches = get_arg_matcher().get_matches_from(args);
        KafkaProducerConfig::from_matches(&matches)
    }

    pub fn from_matches(matches: &ArgMatches) -> KafkaProducerConfig {
        let brokers = matches
            .value_of("brokers")
            .expect("Must specify brokers")
            .to_owned();
        let group_id = matches.value_of("group-id").unwrap_or("kafcat").to_owned();
        let partition = matches
            .value_of("partition")
            .map(|x| x.parse().expect("Cannot parse partition"));
        let topic = matches
            .value_of("topic")
            .expect("Must specify topic")
            .to_owned();
        let msg_delim = matches.value_of("msg-delimiter").unwrap().to_owned();
        let key_delim = matches.value_of("key-delimiter").unwrap().to_owned();
        let format = matches.value_of("format").expect("Must specify format");
        KafkaProducerConfig {
            brokers,
            group_id,
            partition,
            topic,
            msg_delim,
            key_delim,
            format: SerdeFormat::from_str(format).unwrap(),
        }
    }
}

impl Default for KafkaProducerConfig {
    #[rustfmt::skip]
    fn default() -> Self {
        KafkaProducerConfig {
            brokers:   BROKERS_DEFAULT.to_string(),
            group_id:  GROUP_ID_DEFAULT.to_string(),
            partition: None,
            topic:     "".to_string(),
            msg_delim: MSG_DELIMITER_DEFAULT.to_string(),
            key_delim: KEY_DELIMITER_DEFAULT.to_string(),
            format:    SerdeFormat::from_str(FORMAT_DEFAULT).unwrap(),
        }
    }
}
#[cfg(test)]
mod tests {
    use crate::configs::KafkaConsumerConfig;
    use crate::configs::KafkaOffset;
    use crate::configs::KafkaProducerConfig;

    #[test]
    fn consumer_config() {
        let config = KafkaConsumerConfig::from_args(&vec![
            "kafcat",
            "-C",
            "-b",
            "localhost",
            "-t",
            "topic",
            "-e",
        ]);
        assert_eq!(
            config,
            KafkaConsumerConfig {
                brokers: "localhost".to_string(),
                group_id: "kafcat".to_string(),
                offset: KafkaOffset::Beginning,
                partition: None,
                topic: "topic".to_string(),
                exit_on_done: true,
                ..Default::default()
            }
        )
    }
    #[test]
    fn producer_config() {
        let config =
            KafkaProducerConfig::from_args(&vec!["kafcat", "-P", "-b", "localhost", "-t", "topic"]);
        assert_eq!(
            config,
            KafkaProducerConfig {
                brokers: "localhost".to_string(),
                group_id: "kafcat".to_string(),
                partition: None,
                topic: "topic".to_string(),
                ..Default::default()
            }
        )
    }
    #[test]
    fn copy_config() {
        let config = vec![
            "kafcat",
            "copy",
            "-b",
            "localhost1",
            "-t",
            "topic1",
            "-e",
            "--",
            "-b",
            "localhost2",
            "-t",
            "topic2",
        ];
        let consumer_config = KafkaConsumerConfig::from_args(&config);
        let producer_config = KafkaProducerConfig::from_args(&config);
        assert_eq!(
            consumer_config,
            KafkaConsumerConfig {
                brokers: "localhost1".to_string(),
                group_id: "kafcat".to_string(),
                offset: KafkaOffset::Beginning,
                partition: None,
                topic: "topic1".to_string(),
                exit_on_done: true,
                ..Default::default()
            }
        );
        assert_eq!(
            producer_config,
            KafkaProducerConfig {
                brokers: "localhost2".to_string(),
                group_id: "kafcat".to_string(),
                partition: None,
                topic: "topic2".to_string(),
                ..Default::default()
            }
        );
    }
}
