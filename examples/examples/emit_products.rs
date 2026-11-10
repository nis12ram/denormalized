use serde::{Deserialize, Serialize};
use std::result::Result;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;

#[derive(Serialize, Deserialize)]
struct Product {
    id: i64,
    name: String,
    price: i32,
}

/// Emits fake JSON product records to the `xyz.products` topic for the
/// `sql_create_table` example.
///
/// Each record matches the source schema declared in `sql_create_table`:
/// `{ "id": <i64>, "name": <string>, "price": <i32> }`.
///
/// Steps to follow:
/// 1. Start a local Kafka broker on `localhost:9092` (Terminal 1).
/// 2. From the repository root, start this producer (Terminal 2):
///    `cargo run -p denormalized-examples --example emit_products`
/// 3. In another terminal, run (Terminal 3):
///    `cargo run -p denormalized-examples --example sql_create_table`
///
/// The producer runs until interrupted with Ctrl+C.
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut tasks = tokio::task::JoinSet::new();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", String::from("localhost:9092"))
        .set("message.timeout.ms", "100")
        .create()
        .expect("Producer creation error");

    let next_id = Arc::new(AtomicI64::new(1));

    for task_idx in 0..20u64 {
        let producer = producer.clone();
        let next_id = Arc::clone(&next_id);

        tasks.spawn(async move {
            let mut rng = Rng::new(task_idx + 1);

            loop {
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                let product = fake_product(&mut rng, id);

                let (topic, msg) = (
                    "xyz.products".to_string(),
                    serde_json::to_vec(&product).unwrap(),
                );

                let _ = producer
                    .send_result(FutureRecord::<(), Vec<u8>>::to(topic.as_str()).payload(&msg))
                    .unwrap()
                    .await
                    .unwrap();

                // Slightly jittered delay so the stream looks less mechanical
                let delay = 50 + (rng.next_u64() % 100);
                tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
            }
        });
    }

    while let Some(res) = tasks.join_next().await {
        let _ = res;
    }

    Ok(())
}

/// Minimal xorshift64* PRNG
struct Rng(u64);

impl Rng {
    fn new(salt: u64) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15);
        // SplitMix-style scramble so similar seeds diverge quickly
        let mut z = nanos ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        Rng(if z == 0 { 0x2545_F491_4F6C_DD1D } else { z })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Random integer in [lo, hi] inclusive.
    fn range(&mut self, lo: i32, hi: i32) -> i32 {
        let span = (hi - lo + 1) as u64;
        lo + (self.next_u64() % span) as i32
    }

    fn pick<'a>(&mut self, items: &'a [&'a str]) -> &'a str {
        items[(self.next_u64() % items.len() as u64) as usize]
    }
}

const ADJECTIVES: &[&str] = &[
    "Ergonomic",
    "Wireless",
    "Compact",
    "Premium",
    "Rustic",
    "Sleek",
    "Durable",
    "Portable",
    "Smart",
    "Classic",
    "Lightweight",
    "Heavy-Duty",
    "Eco-Friendly",
];
const MATERIALS: &[&str] = &[
    "Steel", "Wooden", "Cotton", "Leather", "Plastic", "Bamboo", "Ceramic", "Aluminum", "Glass",
    "Silicone",
];
const ITEMS: &[&str] = &[
    "Keyboard",
    "Mouse",
    "Backpack",
    "Water Bottle",
    "Desk Lamp",
    "Headphones",
    "Chair",
    "Notebook",
    "Speaker",
    "Watch",
    "Wallet",
    "Monitor Stand",
    "Phone Case",
    "Coffee Mug",
    "Yoga Mat",
    "Power Bank",
    "Tripod",
    "Blender",
];

fn fake_product(rng: &mut Rng, id: i64) -> Product {
    let name = format!(
        "{} {} {}",
        rng.pick(ADJECTIVES),
        rng.pick(MATERIALS),
        rng.pick(ITEMS)
    );
    Product {
        id,
        name,
        price: rng.range(99, 99_999), // e.g. price in cents/paise-like units
    }
}
