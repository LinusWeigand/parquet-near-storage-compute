use hyper::body::HttpBody;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use std::convert::Infallible;
use std::path::Path;

const FOLDER: &str = "/mnt/raid0";

#[tokio::main]
async fn main() {
    let addr = "0.0.0.0:5201".parse().unwrap();
    println!("Server listening on {}", addr);

    let make_svc = make_service_fn(|_conn| async {
        Ok::<_, Infallible>(service_fn(move |req| handle_request(req)))
    });

    let server = Server::bind(&addr).serve(make_svc);

    if let Err(e) = server.await {
        eprintln!("Server error: {}", e);
    }
}

async fn handle_request(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    if req.method() != Method::POST {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::from("Only POST requests are allowed"))
            .unwrap());
    }

    let file_name = if let Some(query) = req.uri().query() {
        let params: Vec<&str> = query.split('&').collect();
        let mut file_name = None;
        for param in params {
            let kv: Vec<&str> = param.split('=').collect();
            if kv.len() == 2 && kv[0] == "file_name" {
                file_name = Some(kv[1].to_string());
                break;
            }
        }
        if let Some(name) = file_name {
            name
        } else {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Missing file_name parameter"))
                .unwrap());
        }
    } else {
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from("Missing file_name parameter"))
            .unwrap());
    };

    println!("Receiving file: {}", file_name);

    let file_path = Path::new(FOLDER).join(&file_name);

    let mut body = req.into_body();

    let mut bytes_received = 0;

    while let Some(chunk_result) = body.data().await {
        match chunk_result {
            Ok(chunk) => {
                let data_len = chunk.len();
                bytes_received += data_len;
            }
            Err(e) => {
                eprintln!("Error while reading body: {}", e);
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("Invalid request body"))
                    .unwrap());
            }
        }
    }

    println!("Total bytes received: {}", bytes_received);
    println!(
        "File {} received successfully and saved to {:?}",
        file_name, file_path
    );

    Ok(Response::new(Body::from("File uploaded successfully")))
}
