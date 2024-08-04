
use std::io;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};

#[tokio::main]
async fn main() {
    deleter().await;
}

async fn deleter() {
    let mut buffer = Vec::<String>::with_capacity(1000);

    for line in io::stdin().lines() {
        match line {
            Ok(line) => {
                buffer.push(line);
            }
            Err(e) => {
                eprintln!("Error reading line: {}", e);
            }
        }

        if buffer.len() == buffer.capacity() {
            println!("Buffer is full, flushing...");

            // make http delete request to s3
            flusher(buffer.clone()).await;

            buffer.clear();

        }
    }

    flusher(buffer.clone()).await;
}

async fn flusher(buffer: Vec<String>) {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url("<bucket endpoint here>")
        .load().await;

    let s3_client = aws_sdk_s3::Client::new(&sdk_config);

    let mut delete_objects: Vec<ObjectIdentifier> = vec![];

    for key in buffer {
        delete_objects.push(
            ObjectIdentifier::builder()
                .set_key(Some(key))
                .build()
                .unwrap()
        )
    }

    s3_client.delete_objects()
        .bucket("amhak")
        .delete(
            Delete::builder()
                .set_objects(Some(delete_objects))
                .build()
                .unwrap()
        )
        .send()
        .await
        .unwrap();
}