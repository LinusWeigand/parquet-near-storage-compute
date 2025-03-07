use clap::Parser;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpListener;

const FILE_PATH: &str = "/mnt/raid0/testfile";
const BLOCK_SIZE: usize = 256 * 1024;

struct DataChunk {
    offset: u64,
    data: Vec<u8>,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "4")]
    write: u64,
    #[arg(short, long, default_value = "2")]
    channel_size: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = "0.0.0.0:5201";
    let listener = TcpListener::bind(addr).await?;
    println!("Server listening on {}", addr);

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn({
            async move {
                let mut header = vec![0u8; 256];

                if socket.read_exact(&mut header).await.is_err() {
                    eprintln!("Failed to read header.");
                    return;
                }

                let header_str = String::from_utf8_lossy(&header);
                let header_str = header_str.trim_matches(char::from(0));
                let parts: Vec<&str> = header_str.split('|').collect();
                if parts.len() != 2 {
                    eprintln!("Invalid header format.");
                    return;
                }
                let file_name = parts[0];
                let file_size: u64 = match parts[1].trim().parse() {
                    Ok(size) => size,
                    Err(_) => {
                        eprintln!("Invalid file size in header.");
                        return;
                    }
                };
                println!("Receiving file: {} ({} bytes)", file_name, file_size);

                let (tx, rx) = async_channel::bounded::<Arc<DataChunk>>(4);

                let _write_tasks: Vec<_> = (0..args.write)
                    .map(|_| {
                        let rx = rx.clone();

                        tokio::spawn(async move {
                            let mut file = OpenOptions::new()
                                .write(true)
                                .create(true)
                                .custom_flags(libc::O_DIRECT)
                                .open(FILE_PATH)
                                .await
                                .expect("Failed to open file");

                            while let Ok(data_chunk) = rx.recv().await {
                                file.seek(SeekFrom::Start(data_chunk.offset))
                                    .await
                                    .expect("Seek failed");
                                file.write_all(&data_chunk.data)
                                    .await
                                    .expect("Write failed");
                            }
                        });
                    })
                    .collect();

                let mut offset = 0u64;
                let mut received = 0u64;

                let mut buffer = [0u8; BLOCK_SIZE];

                while received < file_size {
                    let n = match socket.read(&mut buffer).await {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("Failed to read from socket: {}", e);
                            break;
                        }
                    };

                    let data_chunk = Arc::new(DataChunk {
                        offset: offset,
                        data: buffer[..n].to_vec(),
                    });

                    if tx.send(Arc::clone(&data_chunk)).await.is_err() {
                        eprintln!("Failed to send data to writer task.");
                        return;
                    }

                    received += n as u64;
                    offset += n as u64;
                }

                drop(tx);

                println!(
                    "File {} received successfully and saved to {:?}",
                    file_name, FILE_PATH
                );
            }
        });
    }
}
