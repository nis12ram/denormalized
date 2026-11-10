use denormalized::common::error::Result;
use denormalized::context::Context;

/// Exercises `CREATE TABLE` using a local Kafka broker.
///
/// The example creates a `product` source table from JSON records and prints
/// the decoded stream.
///
/// Steps to follow:
/// 1. Start a local Kafka broker on `localhost:9092` (Terminal 1).
/// 2. From the repository root, start the producer (Terminal 2):
///    `cargo run -p denormalized-examples --example emit_products`
/// 3. In another terminal, run (Terminal 3):
///    `cargo run -p denormalized-examples --example sql_create_table`
///
/// The producer writes JSON records to the `xyz.products` topic. The topic is
/// auto-created by the producer when the broker allows it; create it manually
/// if topic auto-creation is disabled.
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let config = Context::default_config().set_bool("denormalized_config.checkpoint", false);

    let ctx = Context::with_config(config)?;
    ctx.execute_sql(
        "CREATE TABLE product ( \
            id BIGINT, \
            name STRING, \
            price INT \
        ) \
        WITH ( \
            connector = 'kafka', \
            topic = 'xyz.products', \
            'bootstrap.servers' = 'localhost:9092', \
            encoding = 'json', \
            'auto.offset.reset' = 'latest', \
            'group.id' = 'sample_pipeline' \
        )",
    )
    .await?;

    let ds = ctx.from_table("product").await?;
    ds.print_stream().await?;

    Ok(())
}
