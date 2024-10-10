use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig,
    Message,
};
use std::env::args;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stdout = tokio::io::stdout();
    stdout.write(b"Welcome to Kafka chat!\n").await.unwrap();

    // create the consumer
    let consumer = create_consumer(&args().skip(1).next()
        .unwrap_or("localhost:9092".to_string()));

    // subscribe to our topic
    consumer.subscribe(&["chat"])?;

    loop {
        match consumer.recv().await {
            Ok(message) => {
                let payload = message.payload().ok_or_else(|| "no payload for message")?;
                stdout.write(b"> ").await?;
                stdout.write(payload).await?;
                stdout.write(b"\n").await?;
            }
            Err(e) => println!("Kafka error: {}", e),
        }
    }
}

fn create_consumer(bootstrap_server: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("enable.partition.eof", "false")
        .set("group.id", "chat-v1")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Failed to create client")
}