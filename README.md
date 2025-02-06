# reth-exex-indexer

⚠️ **WARNING: This is an alpha version, not ready !**

A Rust-based Ethereum execution layer indexer built on top of reth. Currently supports indexing:
- ERC20 transfers
- Contract deployments
- Native transfers (ETH)
- Execution traces

## Requirements
- Rust
- ClickHouse database
- Environment variable `DATABASE_URL` pointing to your ClickHouse instance

## Status
Work in progress, alpha version. Breaking changes may occur at any time. 