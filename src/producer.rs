use rdkafka::producer::FutureProducer;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::env::args;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

#[tokio::main]
async fn main() {
    let mut stdout = tokio::io::stdout();
    // let mut input_lines = BufReader::new(tokio::io::stdin()).lines();

    stdout.write(b"Welcome to Kafka chat!\n").await.unwrap();


    // Creates a producer, reading the bootstrap server from the first command-line argument
    // or defaulting to localhost:9092
    let producer = create_producer(
        &args().skip(1).next()
            .unwrap_or("192.168.24.228:9092".to_string())
    );

    let mut input_lines = BufReader::new(tokio::io::stdin()).lines();

    loop {
        // Write a prompt to stdout
        stdout.write(b"> ").await.unwrap();
        stdout.flush().await.unwrap();

        // Read a line from stdin
        match input_lines.next_line().await.unwrap() {
            Some(line) => {
                // Send the line to Kafka on the 'chat' topic
                producer.send(rdkafka::producer::FutureRecord::<(), _>::to("chat")
                                  .payload(&line), Timeout::Never)
                    .await
                    .expect("Failed to produce");
            }
            None => break,
        }
    }
}
fn create_producer(bootstrap_server: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("queue.buffering.max.ms", "0")
        .set("enable.partition.eof", "false")
        // We'll give each session its own (unique) consumer group id,
        // so that each session will receive all messages
        .set("group.id", format!("chat-{}", Uuid::new_v4()))
        .create()
        .expect("Failed to create client")
}