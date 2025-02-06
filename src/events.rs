use alloy_primitives::{Address, BlockHash, hex, keccak256};
use async_trait::async_trait;
use chrono::Utc;
use reth_primitives::{
    SealedBlockWithSenders,
    TransactionSigned,
    Receipt,
    Header,
    Withdrawals,
    Log,
};
use clickhouse::{Client, Row};
use reth_tracing::tracing::warn;
use crate::utils::sanitize_bytes;
use std::sync::Arc;
use tokio::sync::Semaphore;
use futures_util::{pin_mut, future::join_all};
use lazy_static::lazy_static;
use primitive_types::{H256, U256};
use alloy_rpc_types_trace::{parity::Action};
use alloy_rpc_types_trace::parity::{LocalizedTransactionTrace, TraceOutput};

lazy_static! {
    static ref ERC20_TRANSFER_TOPIC: H256 = H256(
        <[u8; 32]>::try_from(hex::decode("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            .expect("Static transfer topic decoding failed")).unwrap()
    );
}

pub struct ProcessingResult {
    pub records_written: usize,
}

#[async_trait]
pub trait ProcessingEvent: Send + Sync {
    fn name(&self) -> &'static str;

    /// Process the block data
    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>, block_traces: Option<Vec<LocalizedTransactionTrace>>) -> eyre::Result<ProcessingResult>;

    /// Revert the processed data for given block numbers
    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()>;
}

#[derive(Clone)]
pub struct Erc20TransfersEvent;

impl Erc20TransfersEvent {
    fn is_transfer_event(log: &Log) -> bool {
        log.topics().len() == 3 &&
            log.data.data.len() == 32 &&
            log.topics().get(0).map_or(false, |topic| topic == ERC20_TRANSFER_TOPIC.as_ref())
    }

    fn parse_transfer_event(log: &Log) -> Option<(Address, Address, String)> {
        let from_address = Address::from_slice(&log.topics()[1].as_slice()[12..]);
        let to_address = Address::from_slice(&log.topics()[2].as_slice()[12..]);
        let value = U256::from_big_endian(&log.data.data).to_string();
        Some((from_address, to_address, value))
    }
}

#[async_trait]
impl ProcessingEvent for Erc20TransfersEvent {
    fn name(&self) -> &'static str {
        "Erc20TransfersEvent"
    }

    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>, _block_traces: Option<Vec<LocalizedTransactionTrace>>) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let receipts = &block_data.1;
        let block_number = block.block.header.header().number;
        let block_hash = block.block.header.hash();

        let mut records_written = 0;

        for (tx_idx, (tx, receipt)) in block.block.body.transactions.iter().zip(receipts.iter()).enumerate() {
            if let Some(receipt) = receipt {
                for (log_idx, log) in receipt.logs.iter().enumerate() {
                    if Self::is_transfer_event(log) {
                        if let Some((from_address, to_address, value)) = Self::parse_transfer_event(log) {
                            let query = format!(
                                r#"INSERT INTO erc20_transfers VALUES ({}, '{}', {}, {}, '{}', '{}', '{}', '{}', '{}', {}, '{}')"#,
                                block_number,
                                block_hash.to_string(),
                                tx_idx,
                                log_idx,
                                tx.hash().to_string(),
                                log.address.to_string(),
                                from_address.to_string(),
                                to_address.to_string(),
                                value,
                                tx.chain_id().map_or("NULL".to_string(), |id| id.to_string()),
                                Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                            );

                            client.query(&query).fetch_all().await?;
                            records_written += 1;
                        }
                    }
                }
            }
        }

        Ok(ProcessingResult { records_written })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        let block_numbers_str = block_numbers.iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
            
        let query = format!(
            "ALTER TABLE erc20_transfers DELETE WHERE block_number IN ({})",
            block_numbers_str
        );
        
        client.query(&query).fetch_all().await?;
        
        Ok(())
    }
}

#[derive(Clone)]
pub struct TracesEvent;

#[async_trait]
impl ProcessingEvent for TracesEvent {
    fn name(&self) -> &'static str {
        "TracesEvent"
    }

    async fn process(
        &self,
        block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
        client: &Arc<Client>,
        block_traces: Option<Vec<LocalizedTransactionTrace>>,
    ) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let block_number = block.block.header.header().number;
        let block_hash = block.block.header.hash();
        let receipts = &block_data.1;

        let traces = match block_traces {
            Some(traces) => traces,
            None => return Ok(ProcessingResult { records_written: 0 }),
        };

        let mut records_written = 0;

        for trace in traces {
            let trace_address = trace.trace.trace_address
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(",");

            let tx_success = trace.transaction_hash
                .and_then(|tx_hash| {
                    block.block.body.transactions.iter()
                        .position(|tx| tx.hash() == tx_hash)
                        .and_then(|idx| receipts.get(idx))
                        .and_then(|r| r.as_ref())
                        .map(|receipt| receipt.success)
                });

            let (
                action_type,
                from,
                to,
                value,
                gas,
                input,
                output,
                success,
                deployed_contract_address,
                deployed_contract_code,
                call_type,
                reward_type,
                gas_used,
            ) = match &trace.trace.action {
                Action::Call(call) => (
                    "call",
                    Some(call.from.to_string()),
                    Some(call.to.to_string()),
                    Some(call.value.to_string()),
                    Some(call.gas as i64),
                    Some(hex::encode(&call.input)),
                    trace.trace.result.as_ref().map(|r| match r {
                        TraceOutput::Call(call) => hex::encode(&call.output),
                        _ => String::new(),
                    }),
                    trace.trace.result.is_some(),
                    None,
                    None,
                    Some(format!("{:?}", call.call_type)),
                    None,
                    trace.trace.result.as_ref().map(|r| r.gas_used() as i64),
                ),
                Action::Create(create) => (
                    "create",
                    Some(create.from.to_string()),
                    None,
                    Some(create.value.to_string()),
                    Some(create.gas as i64),
                    Some(hex::encode(&create.init)),
                    trace.trace.result.as_ref().map(|r| match r {
                        TraceOutput::Create(create) => hex::encode(&create.code),
                        _ => String::new(),
                    }),
                    trace.trace.result.is_some(),
                    trace.trace.result.as_ref().and_then(|r| match r {
                        TraceOutput::Create(create) => Some(create.address.to_string()),
                        _ => None,
                    }),
                    trace.trace.result.as_ref().and_then(|r| match r {
                        TraceOutput::Create(create) => Some(hex::encode(&create.code)),
                        _ => None,
                    }),
                    None,
                    None,
                    trace.trace.result.as_ref().map(|r| r.gas_used() as i64),
                ),
                Action::Selfdestruct(selfdestruct) => (
                    "selfdestruct",
                    Some(selfdestruct.address.to_string()),
                    Some(selfdestruct.refund_address.to_string()),
                    Some(selfdestruct.balance.to_string()),
                    None,
                    None,
                    None,
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
                Action::Reward(reward) => (
                    "reward",
                    None,
                    Some(reward.author.to_string()),
                    Some(reward.value.to_string()),
                    None,
                    None,
                    None,
                    true,
                    None,
                    None,
                    None,
                    Some(format!("{:?}", reward.reward_type)),
                    None,
                ),
            };

            let query = format!(
                r#"INSERT INTO traces VALUES ({}, '{}', {}, {}, '{}', {}, '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}')"#,
                block_number,
                block_hash.to_string(),
                trace.transaction_hash.map_or("NULL".to_string(), |h| format!("'{}'", h.to_string())),
                trace.transaction_position.unwrap_or(0),
                trace_address,
                trace.trace.subtraces,
                action_type,
                from.map_or("NULL".to_string(), |s| format!("'{}'", s)),
                to.map_or("NULL".to_string(), |s| format!("'{}'", s)),
                value.map_or("NULL".to_string(), |s| format!("'{}'", s)),
                gas.map_or("NULL".to_string(), |g| g.to_string()),
                gas_used.map_or("NULL".to_string(), |g| g.to_string()),
                input.map_or("NULL".to_string(), |s| format!("'{}'", s)),
                output.map_or("NULL".to_string(), |s| format!("'{}'", s)),
                if success { 1 } else { 0 },
                tx_success.map_or("NULL".to_string(), |s| if s { "1" } else { "0" }),
                trace.trace.error.map_or("NULL".to_string(), |e| format!("'{}'", e.to_string())),
                deployed_contract_address.map_or("NULL".to_string(), |s| format!("'{}'", s)),
                deployed_contract_code.map_or("NULL".to_string(), |s| format!("'{}'", s)),
                call_type.map_or("NULL".to_string(), |s| format!("'{}'", s)),
                reward_type.map_or("NULL".to_string(), |s| format!("'{}'", s)),
                Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            );

            client.query(&query).fetch_all().await?;
            records_written += 1;
        }

        Ok(ProcessingResult { records_written })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        let block_numbers_str = block_numbers.iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
            
        let query = format!(
            "ALTER TABLE traces DELETE WHERE block_number IN ({})",
            block_numbers_str
        );
        
        client.query(&query).fetch_all().await?;
        
        Ok(())
    }
}

#[derive(Clone)]
pub struct NativeTransfersEvent;

#[async_trait]
impl ProcessingEvent for NativeTransfersEvent {
    fn name(&self) -> &'static str {
        "NativeTransfersEvent"
    }

    async fn process(
        &self,
        block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
        client: &Arc<Client>,
        block_traces: Option<Vec<LocalizedTransactionTrace>>,
    ) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let block_number = block.block.header.header().number;
        let block_hash = block.block.header.hash();

        let mut transfer_index = 0;
        let mut records_written = 0;

        if let Some(traces) = block_traces {
            for trace in traces {
                match &trace.trace.action {
                    Action::Call(call) if !call.value.is_zero() => {
                        let query = format!(
                            r#"INSERT INTO native_transfers VALUES ({}, '{}', {}, {}, {}, '{}', '{}', '{}', '{}', '{}')"#,
                            block_number,
                            block_hash.to_string(),
                            trace.transaction_position.map_or("NULL".to_string(), |p| p.to_string()),
                            transfer_index,
                            trace.transaction_hash.map_or("NULL".to_string(), |h| format!("'{}'", h.to_string())),
                            call.from.to_string(),
                            call.to.to_string(),
                            call.value.to_string(),
                            if trace.trace.trace_address.is_empty() { "transaction" } else { "internal_call" },
                            Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                        );

                        client.query(&query).fetch_all().await?;
                        transfer_index += 1;
                        records_written += 1;
                    },
                    Action::Create(create) if !create.value.is_zero() => {
                        let to_address = trace.trace.result
                            .as_ref()
                            .and_then(|r| match r {
                                TraceOutput::Create(create) => Some(create.address.to_string()),
                                _ => None,
                            })
                            .unwrap_or_else(|| "0x0".to_string());

                        let query = format!(
                            r#"INSERT INTO native_transfers VALUES ({}, '{}', {}, {}, {}, '{}', '{}', '{}', '{}', '{}')"#,
                            block_number,
                            block_hash.to_string(),
                            trace.transaction_position.map_or("NULL".to_string(), |p| p.to_string()),
                            transfer_index,
                            trace.transaction_hash.map_or("NULL".to_string(), |h| format!("'{}'", h.to_string())),
                            create.from.to_string(),
                            to_address,
                            create.value.to_string(),
                            "contract_creation",
                            Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                        );

                        client.query(&query).fetch_all().await?;
                        transfer_index += 1;
                        records_written += 1;
                    },
                    Action::Selfdestruct(selfdestruct) if !selfdestruct.balance.is_zero() => {
                        let query = format!(
                            r#"INSERT INTO native_transfers VALUES ({}, '{}', {}, {}, {}, '{}', '{}', '{}', '{}', '{}')"#,
                            block_number,
                            block_hash.to_string(),
                            trace.transaction_position.map_or("NULL".to_string(), |p| p.to_string()),
                            transfer_index,
                            trace.transaction_hash.map_or("NULL".to_string(), |h| format!("'{}'", h.to_string())),
                            selfdestruct.address.to_string(),
                            selfdestruct.refund_address.to_string(),
                            selfdestruct.balance.to_string(),
                            "selfdestruct",
                            Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                        );

                        client.query(&query).fetch_all().await?;
                        transfer_index += 1;
                        records_written += 1;
                    },
                    Action::Reward(reward) => {
                        let query = format!(
                            r#"INSERT INTO native_transfers VALUES ({}, '{}', NULL, {}, NULL, '0x0', '{}', '{}', '{}_reward', '{}')"#,
                            block_number,
                            block_hash.to_string(),
                            transfer_index,
                            reward.author.to_string(),
                            reward.value.to_string(),
                            format!("{:?}", reward.reward_type),
                            Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                        );

                        client.query(&query).fetch_all().await?;
                        transfer_index += 1;
                        records_written += 1;
                    },
                    _ => {}
                }
            }
        }

        Ok(ProcessingResult { records_written })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        let block_numbers_str = block_numbers.iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
            
        let query = format!(
            "ALTER TABLE native_transfers DELETE WHERE block_number IN ({})",
            block_numbers_str
        );
        
        client.query(&query).fetch_all().await?;
        
        Ok(())
    }
}

#[derive(Clone)]
pub struct ContractsEvent;

impl ContractsEvent {
    fn find_deployer(traces: &[LocalizedTransactionTrace], current_trace: &LocalizedTransactionTrace) -> Option<Address> {
        current_trace.transaction_hash.and_then(|tx_hash| {
            traces.iter()
                .find(|trace|
                    trace.transaction_hash == Some(tx_hash) &&
                    trace.trace.trace_address.is_empty()
                )
                .and_then(|trace| match &trace.trace.action {
                    Action::Call(call) => Some(call.from),
                    Action::Create(create) => Some(create.from),
                    _ => None,
                })
        })
    }
}

#[async_trait]
impl ProcessingEvent for ContractsEvent {
    fn name(&self) -> &'static str {
        "ContractsEvent"
    }

    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>, block_traces: Option<Vec<LocalizedTransactionTrace>>) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let block_number = block.block.header.header().number;
        let block_hash = block.block.header.hash();

        let traces = match block_traces {
            Some(traces) => traces,
            None => return Ok(ProcessingResult { records_written: 0 }),
        };

        let mut records_written = 0;
        let mut create_index = 0;

        for trace in &traces {
            if let Action::Create(create) = &trace.trace.action {
                let init_code = &create.init;
                let init_code_hash = keccak256(init_code);

                let (contract_address, deployed_code) = match &trace.trace.result {
                    Some(TraceOutput::Create(result)) => {
                        (result.address.to_vec(), result.code.clone())
                    },
                    _ => continue,
                };

                let code_hash = keccak256(&deployed_code);

                let deployer = Self::find_deployer(&traces, trace)
                    .map(|addr| addr.to_vec())
                    .unwrap_or_else(|| vec![0; 20]);

                let factory = create.from.to_vec();

                let query = format!(
                    r#"INSERT INTO contracts VALUES ({}, '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, '{}', {}, '{}')"#,
                    block_number,
                    block_hash.to_string(),
                    create_index,
                    trace.transaction_hash.map_or("NULL".to_string(), |h| h.to_string()),
                    Address::from_slice(&contract_address).to_string(),
                    Address::from_slice(&deployer).to_string(),
                    Address::from_slice(&factory).to_string(),
                    hex::encode(init_code),
                    hex::encode(&deployed_code),
                    hex::encode(&init_code_hash),
                    init_code.len(),
                    deployed_code.len(),
                    hex::encode(&code_hash),
                    trace.transaction_hash
                        .and_then(|_| block.block.body.transactions[trace.transaction_position.unwrap_or(0) as usize].chain_id())
                        .map_or("NULL".to_string(), |id| id.to_string()),
                    Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                );

                client.query(&query).fetch_all().await?;
                create_index += 1;
                records_written += 1;
            }
        }

        Ok(ProcessingResult { records_written })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        let block_numbers_str = block_numbers.iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
            
        let query = format!(
            "ALTER TABLE contracts DELETE WHERE block_number IN ({})",
            block_numbers_str
        );
        
        client.query(&query).fetch_all().await?;
        
        Ok(())
    }
}

// Event enum
#[derive(Clone)]
pub enum Event {
    Erc20Transfers(Erc20TransfersEvent),
    Traces(TracesEvent),
    NativeTransfers(NativeTransfersEvent),
    Contracts(ContractsEvent),
}

impl Event {
    pub fn name(&self) -> &'static str {
        match self {
            Event::Erc20Transfers(e) => e.name(),
            Event::Traces(e) => e.name(),
            Event::NativeTransfers(e) => e.name(),
            Event::Contracts(e) => e.name(),
        }
    }

    pub async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>, block_traces: Option<Vec<LocalizedTransactionTrace>>) -> eyre::Result<ProcessingResult> {
        match self {
            Event::Erc20Transfers(e) => e.process(block_data, client, block_traces).await,
            Event::Traces(e) => e.process(block_data, client, block_traces).await,
            Event::NativeTransfers(e) => e.process(block_data, client, block_traces).await,
            Event::Contracts(e) => e.process(block_data, client, block_traces).await,
        }
    }

    pub async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        match self {
            Event::Erc20Transfers(e) => e.revert(block_numbers, client).await,
            Event::Traces(e) => e.revert(block_numbers, client).await,
            Event::NativeTransfers(e) => e.revert(block_numbers, client).await,
            Event::Contracts(e) => e.revert(block_numbers, client).await,
        }
    }

    pub fn all() -> Vec<Self> {
        vec![
            Event::Erc20Transfers(Erc20TransfersEvent),
            Event::Traces(TracesEvent),
            Event::NativeTransfers(NativeTransfersEvent),
            Event::Contracts(ContractsEvent),
        ]
    }
}