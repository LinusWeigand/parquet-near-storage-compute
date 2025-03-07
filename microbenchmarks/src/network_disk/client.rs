use rand::{rng, Rng};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::time::{Duration, Instant};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    ip: String,
    #[arg(short, long, default_value = "8")]
    streams: usize,
    #[arg(short, long, default_value = "24576000000")]
    file_size: u64,
    #[arg(short, long, default_value = "60")]
    duration: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let server_ip = format!("{}:5201", args.ip);

    println!(
        "Starting test: server={}, streams={}, duration={}s",
        server_ip, args.streams, args.duration
    );

    let barrier = Arc::new(Barrier::new(args.streams));
    let mut tasks = vec![];

    for i in 0..args.streams {
        let server_ip = server_ip.to_string();
        let barrier = barrier.clone();
        let file_name = format!("file_{}.txt", i + 1);
        let file_size = args.file_size;
        let duration = args.duration;

        let task = tokio::spawn(async move {
            let mut stream = TcpStream::connect(server_ip).await.unwrap();
            let mut header = format!("{}|{}\n", file_name, file_size).into_bytes();

            // Pad header to 256 bytes
            if header.len() < 256 {
                header.resize(256, 0);
            }

            stream.write_all(&header).await.unwrap();

            barrier.wait().await;

            let start_time = Instant::now();
            let buffer = create_random_buffer(64 * 1024);

            let mut sent = 0;
            while start_time.elapsed() < Duration::from_secs(duration) && sent < file_size {
                let to_send = (file_size - sent).min(buffer.len() as u64) as usize;
                stream.write_all(&buffer[..to_send]).await.unwrap();
                sent += to_send as u64;
            }
            println!("Stream for {} completed.", file_name);
        });

        tasks.push(task);
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
