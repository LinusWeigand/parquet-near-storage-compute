use bytes::Bytes;
use clap::Parser;
use futures_util::stream;
use rand::{rng, Rng};
use reqwest::{Body, Client};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long)]
    ip: String,

    #[arg(short, long, default_value = "8")]
    streams: usize,

    #[arg(short, long, default_value = "24576000000")]
    file_size: u64,

    #[arg(short, long, default_value = "60")]
    duration: u64,

    #[arg(short, long, default_value = "512")]
    size: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let block_size: usize = args.size as usize * 1024;
    let server_url = format!("http://{}:5201", args.ip);

    println!(
        "Starting test: server={}, streams={}, duration={}s, file_size={} bytes/stream",
        server_url, args.streams, args.duration, args.file_size
    );

    let client = Client::new();

    let barrier = Arc::new(Barrier::new(args.streams));
    let mut tasks = Vec::with_capacity(args.streams);

    for i in 0..args.streams {
        let client = client.clone();
        let barrier = barrier.clone();
        let file_name = format!("file_{}.txt", i + 1);
        let file_size = args.file_size;
        let duration = args.duration;
        let server_url = server_url.clone();

        let handle = tokio::spawn(async move {
            let url = format!(
                "{}/upload?file_name={}&file_size={}",
                server_url, file_name, file_size
            );

            barrier.wait().await;

            let start_time = Instant::now();

            let buffer = create_random_buffer(block_size);
            let chunk_buffer = Arc::new(buffer);

            let initial_state = (0u64, start_time);

            let data_stream = stream::unfold(initial_state, move |(mut sent, start_time)| {
                let chunk_buffer = Arc::clone(&chunk_buffer);
                async move {
                    let elapsed = start_time.elapsed();
                    if elapsed >= Duration::from_secs(duration) || sent >= file_size {
                        return None;
                    }

                    let remaining = file_size - sent;
                    let chunk_size = std::cmp::min(chunk_buffer.len() as u64, remaining) as usize;

                    let chunk = Bytes::copy_from_slice(&chunk_buffer[..chunk_size]);

                    sent += chunk_size as u64;

                    Some((
                        Ok::<Bytes, Box<dyn Error + Send + Sync>>(chunk),
                        (sent, start_time),
                    ))
                }
            });

            let response = client
                .post(&url)
                .body(Body::wrap_stream(data_stream))
                .send()
                .await;

            match response {
                Ok(resp) => {
                    println!(
                        "Stream for {} completed with status: {}",
                        file_name,
                        resp.status()
                    );
                }
                Err(e) => {
                    eprintln!("Stream for {} failed: {}", file_name, e);
                }
            }
        });

        tasks.push(handle);
    }

    for task in tasks {
        let _ = task.await;
    }

    println!("Test completed.");
    Ok(())
}

fn create_random_buffer(size: usize) -> Vec<u8> {
    let mut buffer = vec![0u8; size];
    rng().fill(&mut buffer[..]);
    buffer
}
