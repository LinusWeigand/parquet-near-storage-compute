use clap::Parser;
use rand::{rng, Rng};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;

const FILE_PATH: &str = "/mnt/raid0/testfile";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "1024")]
    size: u64,
    #[arg(short, long, default_value = "4")]
    write: u64,
    #[arg(short, long, default_value = "50")]
    duration: u64,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let duration = args.duration;
    let block_size: usize = args.size as usize * 1024;
    let total_throughput_mib = Arc::new(Mutex::new(0f64));

    let write_tasks: Vec<_> = (0..args.write)
        .map(|i| {
            let total_throughput_mib = Arc::clone(&total_throughput_mib);
            let file_path = format!("{}{}", FILE_PATH, i);

            tokio::spawn(async move {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(file_path)
                    .await
                    .expect("Failed to open file");
                file.seek(SeekFrom::Start(0)).await.expect("Seek failed");

                let start_time = Instant::now();
                let mut bytes_written = 0usize;

                while start_time.elapsed().as_secs() < duration {
                    let data = create_random_buffer(block_size);
                    file.write_all(&data).await.expect("Write failed");

                    bytes_written += block_size;
                }
                let millis = start_time.elapsed().as_millis();
                let mib_written: f64 = bytes_written as f64 / 1024. / 1024.;
                let secs: f64 = millis as f64 / 1000.;
                let throughput = mib_written / secs;
                println!("Secs: {}", secs);
                println!(
                    "Task {}: Bytes: {}, MiB/s: {}",
                    i, bytes_written, throughput,
                );
                {
                    let mut total = total_throughput_mib.lock().await;
                    *total += throughput;
                }
            })
        })
        .collect();

    futures::future::join_all(write_tasks).await;
    let total_throughput = *total_throughput_mib.lock().await;
    println!("Throughput: {:.2} MiB/s", total_throughput);

    Ok(())
}

fn create_random_buffer(size: usize) -> Vec<u8> {
    let mut buffer = vec![0u8; size];
    rng().fill(&mut buffer[..]);
    buffer
}
