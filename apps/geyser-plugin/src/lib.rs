use anyhow::{Context, Result};
use lazy_static::lazy_static;
use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::producer::{BaseProducer, BaseRecord};
use rdkafka::producer::Producer;
use serde::Serialize;
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaEntryInfo, Result as GeyserResult,
};
use agave_logger::setup_with_default;
use std::sync::Mutex;
use std::time::Duration;


#[derive(Serialize)]
struct EntryEvent {
    slot: u64,
    idx: usize,
    is_full_entry: bool,
}

struct RaywatchGeyserPlugin {
    producer : Option<BaseProducer>,
    topic : String, 
}

impl RaywatchGeyserPlugin {

    fn init_kafka(&mut self, brokers: &str) -> Result<()>{
        let producer : BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;
        self.producer = Some(producer);
        Ok(())
    }

    fn send_entry_event(&self, info: &ReplicaEntryInfo) {
        if let Some(producer) = &self.producer {
            let event = EntryEvent {
                slot: info.slot,
                idx: info.index,
                is_full_entry: info.is_full_entry(),
            };
            match serde_json::to_vec(&event) {
                Ok(payload) => {
                    let record = BaseRecord::to(&self.topic)
                        .key(&info.slot.to_be_bytes())
                        .payload(&payload);

                    if let Err((e, _owned_msg)) = producer.send(record) {
                        error!("RaywatchGeyserPlugin: failed to send to Kafka: {e}");
                    }
                    producer.flush(Duration::from_millis(0));
                }
                Err(e) => {
                    error!("RaywatchGeyserPlugin: failed to serialize event: {e}");
                }
            }
        }
    }
}