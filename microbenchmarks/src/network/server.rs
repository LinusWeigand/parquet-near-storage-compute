use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

const BLOCK_SIZE: usize = 256 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:5201";
    let listener = TcpListener::bind(addr).await?;
    println!("Server listening on {}", addr);

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn({
            async move {
                let mut buffer = vec![0u8; BLOCK_SIZE];
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

                let mut received = 0u64;

                while received < file_size {
                    let n = match socket.read(&mut buffer).await {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("Failed to read from socket: {}", e);
                            break;
                        }
                    };
                    received += n as u64;
                }
            }
        });
    }
}
