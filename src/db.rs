use std::env;
use clickhouse::Client;
use reth_tracing::tracing::info;

pub async fn connect_to_clickhouse() -> eyre::Result<Client> {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let client = Client::default()
        .with_url(&db_url);
    Ok(client)
}

pub async fn create_tables(client: &Client) -> eyre::Result<()> {
    for query in vec![
        r#"
        CREATE TABLE IF NOT EXISTS erc20_transfers (
            block_number                UInt64,
            block_hash                  String,
            transaction_index           UInt32,
            log_index                   UInt32,
            transaction_hash            String,
            erc20                       String,
            from_address                String,
            to_address                  String,
            value                       String,
            chain_id                    Nullable(UInt64),
            updated_at                  DateTime64(3, 'UTC')
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (block_number, transaction_index, log_index)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS traces (
            block_number                UInt64,
            block_hash                  String,
            transaction_hash            Nullable(String),
            transaction_index           UInt32,
            trace_address               String,
            subtraces                   UInt32,
            action_type                 String,
            from_address                Nullable(String),
            to_address                  Nullable(String),
            value                       Nullable(String),
            gas                         Nullable(UInt64),
            gas_used                    Nullable(UInt64),
            input                       Nullable(String),
            output                      Nullable(String),
            success                     UInt8,
            tx_success                  Nullable(UInt8),
            error                       Nullable(String),
            deployed_contract_address   Nullable(String),
            deployed_contract_code      Nullable(String),
            call_type                   Nullable(String),
            reward_type                 Nullable(String),
            updated_at                  DateTime64(3, 'UTC')
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (block_number, transaction_index)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS native_transfers (
            block_number                UInt64,
            block_hash                  String,
            transaction_index           Nullable(UInt32),
            transfer_index              UInt32,
            transaction_hash            Nullable(String),
            from_address                String,
            to_address                  String,
            value                       String,
            transfer_type               String,
            updated_at                  DateTime64(3, 'UTC')
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (block_number, transfer_index)
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS contracts (
            block_number                UInt64,
            block_hash                  String,
            create_index                UInt32,
            transaction_hash            String,
            contract_address            String,
            deployer                    String,
            factory                     String,
            init_code                   String,
            code                        String,
            init_code_hash              String,
            n_init_code_bytes           UInt32,
            n_code_bytes                UInt32,
            code_hash                   String,
            chain_id                    Nullable(UInt64),
            updated_at                  DateTime64(3, 'UTC')
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (contract_address)
        "#,
    ] {
        client.query(query).fetch_all().await?;
    }

    info!("Initialized database tables");
    Ok(())
}
