use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use serde::{Deserialize, Serialize};
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin,
    GeyserPluginError,
    ReplicaEntryInfo,
    ReplicaEntryInfoVersions,
    Result as GeyserResult,
    ReplicaTransactionInfoVersions,
    ReplicaTransactionInfo,
};
use rdkafka::producer::Producer;
use agave_logger::setup_with_default;
use std::fmt;
use std::time::Duration;

#[derive(Serialize)]
struct EntryEvent {
    slot: u64,
    idx: usize,
    num_hashes: u64,
    executed_tx_count: u64,
}

#[derive(Serialize)]
struct TxEvent {
    slot: u64,
    signature: String,
    is_vote: bool,
}

struct RaywatchGeyserPlugin {
    producer: Option<BaseProducer>,
    topic: String,
}

#[derive(Deserialize)]
struct PluginConfig {
    #[serde(default = "default_kafka_brokers")]
    kafka_brokers: String,
}

fn default_kafka_brokers() -> String {
    "localhost:9092".to_string()
}

impl fmt::Debug for RaywatchGeyserPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaywatchGeyserPlugin")
            .field("topic", &self.topic)
            .finish()
    }
}

impl RaywatchGeyserPlugin {
    fn init_kafka(&mut self, brokers: &str) -> GeyserResult<()> {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;

        self.producer = Some(producer);
        Ok(())
    }

    fn send_tx_event(&self, slot: u64, tx: &ReplicaTransactionInfo) {
        if let Some(producer) = &self.producer {
            let event = TxEvent {
                slot,
                signature: tx.signature.to_string(),
                is_vote: tx.is_vote,
            };

            match serde_json::to_vec(&event) {
                Ok(payload) => {
                    let key = slot.to_be_bytes();

                    let record = BaseRecord::to(&self.topic)
                        .key(&key)
                        .payload(&payload);

                    if let Err((e, _owned_msg)) = producer.send(record) {
                        error!("RaywatchGeyserPlugin: failed to send tx to Kafka: {e}");
                    }

                    // ok for now in dev; later we can optimize
                    let _ = producer.flush(Duration::from_millis(0));
                }
                Err(e) => {
                    error!("RaywatchGeyserPlugin: failed to serialize tx: {e}");
                }
            }
        }
    }

    fn send_entry_event(&self, info: &ReplicaEntryInfo) {
        if let Some(producer) = &self.producer {
            let event = EntryEvent {
                slot: info.slot,
                idx: info.index,
                num_hashes: info.num_hashes,
                executed_tx_count: info.executed_transaction_count,
            };

            match serde_json::to_vec(&event) {
                Ok(payload) => {
                    let key = info.slot.to_be_bytes();

                    let record = BaseRecord::to(&self.topic)
                        .key(&key)
                        .payload(&payload);

                    if let Err((e, _owned_msg)) = producer.send(record) {
                        error!("RaywatchGeyserPlugin: failed to send to Kafka: {e}");
                    }

                    if let Err(e) = producer.flush(Duration::from_millis(0)) {
                        error!("RaywatchGeyserPlugin: flush error: {e}");
                    }
                }
                Err(e) => {
                    error!("RaywatchGeyserPlugin: failed to serialize event: {e}");
                }
            }
        }
    }

    fn handle_tx_versions(
        &self,
        tx: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> GeyserResult<()> {
        match tx {
            ReplicaTransactionInfoVersions::V0_0_1(tx_info) => {
                info!(
                    "RaywatchGeyserPlugin: got tx in slot {slot} (is_vote={})",
                    tx_info.is_vote
                );
                self.send_tx_event(slot, tx_info);
            }

            other => {
                info!(
                    "RaywatchGeyserPlugin: notify_transaction called with unsupported transaction info version at slot {slot}"
                );
            }
        };
        Ok(())
    }

    fn handle_entry_versions(
        &self,
        entry: ReplicaEntryInfoVersions<'_>,
    ) -> GeyserResult<()> {
        match entry {
            ReplicaEntryInfoVersions::V0_0_1(info) => {

                if info.executed_transaction_count == 0 {
                    return Ok(());
                }

                info!(
                    "RaywatchGeyserPlugin: entry slot={} idx={} txs={}",
                    info.slot, info.index, info.executed_transaction_count
                );

                self.send_entry_event(info);
            }

            other => {
                info!(
                    "RaywatchGeyserPlugin: notify_entry called with unsupported entry info version"
                );
            }
        };
        Ok(())
    }
}

impl GeyserPlugin for RaywatchGeyserPlugin {
    fn name(&self) -> &'static str {
        "raywatch_geyser_plugin"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> GeyserResult<()> {
        setup_with_default("info");
        info!("RaywatchGeyserPlugin: loading with config {config_file}");

        let brokers = match std::fs::read_to_string(config_file) {
            Ok(contents) => match serde_json::from_str::<PluginConfig>(&contents) {
                Ok(cfg) => cfg.kafka_brokers,
                Err(e) => {
                    error!(
                        "RaywatchGeyserPlugin: failed to parse config {config_file}: {e}; using default localhost:9092"
                    );
                    default_kafka_brokers()
                }
            },
            Err(e) => {
                error!(
                    "RaywatchGeyserPlugin: failed to read config {config_file}: {e}; using default localhost:9092"
                );
                default_kafka_brokers()
            }
        };

        self.init_kafka(&brokers)?;
        info!("RaywatchGeyserPlugin: connected to Kafka at {brokers}");
        Ok(())
    }

    fn on_unload(&mut self) {
        info!("RaywatchGeyserPlugin: unloading");
        self.producer = None;
    }

    fn notify_transaction(
        &self,
        tx: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> GeyserResult<()> {
        self.handle_tx_versions(tx, slot)
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions<'_>) -> GeyserResult<()> {
        self.handle_entry_versions(entry)
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_notifications_enabled(&self) -> bool {
        false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = RaywatchGeyserPlugin {
        producer: None,
        topic: "raydium-swaps-raw".to_string(),
    };
    Box::into_raw(Box::new(plugin))
}
