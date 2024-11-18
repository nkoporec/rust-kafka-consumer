use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use reqwest::Client;
use std::env;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    // Configure the Kafka consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("group.id", "rust_consumer_group")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Consumer creation failed");

    // Subscribe to a topic
    consumer
        .subscribe(&[
            "com.getopensocial.cms.user.login",
            "com.getopensocial.cms.event.create",
        ])
        .expect("Subscription failed");

    // Set up HTTP client for webhook
    let webhook_url = "http://localhost:3000/webhook";
    let http_client = Client::new();

    println!("Starting to consume messages...");

    // Start consuming messages
    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Kafka error: {}", e),
            Ok(m) => {
                if let Some(payload) = m.payload() {
                    // Convert payload to string
                    let message_text =
                        std::str::from_utf8(payload).unwrap_or("Invalid UTF-8 message");
                    println!("Received message: {}", message_text);

                    // Send message to webhook
                    let send_result =
                        send_to_webhook(&http_client, &webhook_url, message_text).await;

                    if let Err(e) = send_result {
                        eprintln!("Failed to send message to webhook: {:?}", e);
                    }
                }
                consumer
                    .commit_message(&m, rdkafka::consumer::CommitMode::Async)
                    .unwrap();
            }
        }
    }
}

// Function to send the Kafka message to the webhook as JSON
async fn send_to_webhook(client: &Client, url: &str, message: &str) -> Result<(), reqwest::Error> {
    let json_payload = serde_json::json!({
        "message": message
    });

    let response = client.post(url).json(&json_payload).send().await?;

    if response.status().is_success() {
        println!("Successfully sent message to webhook");
    } else {
        eprintln!("Webhook responded with error: {:?}", response.status());
    }

    Ok(())
}
