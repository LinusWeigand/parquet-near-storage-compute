use clap::Parser;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::Mutex;

const FILE_PATH: &str = "/mnt/raid0/testfile";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "1024")]
    size: u64,
    #[arg(short, long, default_value = "4")]
    read: u64,
    #[arg(short, long, default_value = "50")]
    duration: u64,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let duration = args.duration;
    let block_size: usize = args.size as usize * 1024;
    let total_throughput_mib = Arc::new(Mutex::new(0f64));

    let read_tasks: Vec<_> = (0..args.read)
        .map(|i| {
            let total_throughput_mib = Arc::clone(&total_throughput_mib);
            let file_path = format!("{}{}", FILE_PATH, i);

            tokio::spawn(async move {
                let mut file = File::open(file_path)
                    .await
                    .expect("Failed to open file");
                file.seek(SeekFrom::Start(0)).await.expect("Seek failed");

                let start_time = Instant::now();
                let mut bytes_read = 0usize;

                while start_time.elapsed().as_secs() < duration {
                    let mut buffer = vec![0u8; block_size];
                    match file.read_exact(&mut buffer).await {
                        Ok(_) => {
                            bytes_read += block_size;
                        }
                        Err(e) => {
                            eprintln!(
                                "Task {}: read_exact failed with {}. Possibly EOF. Resetting to start.",
                                i, e
                            );
                            file.seek(SeekFrom::Start(0))
                                .await
                                .expect("Seek failed after EOF");
                            continue;
                        }
                    }
                }
                let millis = start_time.elapsed().as_millis();
                let mib_read: f64 = bytes_read as f64 / 1024. / 1024.;
                let secs: f64 = millis as f64 / 1000.;
                let throughput = mib_read / secs;
                println!("Secs: {}", secs);
                println!(
                    "Task {}: Bytes: {}, MiB/s: {}",
                    i, bytes_read, throughput,
                );
                {
                    let mut total = total_throughput_mib.lock().await;
                    *total += throughput;
                }
            })
        })
        .collect();

    futures::future::join_all(read_tasks).await;
    let total_throughput = *total_throughput_mib.lock().await;
    println!("Throughput: {:.2} MiB/s", total_throughput);

    Ok(())
}
