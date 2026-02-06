# Horizons Rust SDK

Async Rust client for the Horizons REST API.

## Install

```bash
cargo add horizons-ai
```

## Quickstart

```rust
use horizons_ai::HorizonsClient;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), horizons_ai::HorizonsError> {
    let client = HorizonsClient::new("http://localhost:8000", Uuid::nil())
        .with_api_key("dev-token");

    let health = client.health().await?;
    println!("{health:?}");
    Ok(())
}
```

