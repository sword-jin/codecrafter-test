// Uncomment this block to pass the first stage
// use std::net::TcpListener;

use anyhow::{Result};
use core::result::Result::Ok;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::{
    collections::HashMap,
    env,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};

const RESPONSE_OK: &str = "HTTP/1.1 200 OK\r\n";
const RESPONSE_CREATED: &str = "HTTP/1.1 201 Created\r\n";
const RESPONSE_NOT_FOUND: &str = "HTTP/1.1 404 Not Found\r\n";
const RESPONSE_BAD_REQUEST: &str = "HTTP/1.1 400 Bad Request\r\n";
const EOL: &str = "\r\n";

fn main() {
    let args = env::args().collect::<Vec<String>>();
    let mut directory = ".";
    // if args[1] is "--directory", then change the directory to args[2]
    if args.get(1).unwrap_or(&"".to_string()) == "--directory" {
        directory = args[2].as_str();
    }

    let listener = TcpListener::bind("127.0.0.1:4221").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let result = handle_connection(stream, &directory);
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        println!("error: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream, directory: &str) -> Result<()> {
    let req = parse_request(&stream)?;

    println!("Request: {:?}", req);

    match (req.method.as_str(), req.path.as_str()) {
        (_, "/") => {
            stream.write_all(RESPONSE_OK.as_bytes())?;
            stream.write_all(EOL.as_bytes())?;
        }
        (_, "/user-agent") => {
            let header_map = req.header_map();

            let user_agent = header_map.get("User-Agent");
            if user_agent.is_none() {
                stream.write_all(RESPONSE_BAD_REQUEST.as_bytes())?;
                return Ok(());
            }
            stream.write_all(RESPONSE_OK.as_bytes())?;
            add_header(&mut stream, ("Content-Type", "text/plain"))?;
            write_body(&mut stream, &user_agent.unwrap().join(","))?;
        }
        ("GET", _) if req.path.starts_with("/files/") => {
            let filename = &req.path[7..];
            if filename.is_empty() {
                stream.write_all(RESPONSE_BAD_REQUEST.as_bytes())?;
                return Ok(());
            }

            let filepath = format!("{}/{}", directory, filename);
            if !std::path::Path::new(&filepath).exists() {
                stream.write_all(RESPONSE_NOT_FOUND.as_bytes())?;
                stream.write_all(EOL.as_bytes())?;
                return Ok(());
            }

            let file_buffer = std::fs::read(filepath)?;

            stream.write_all(RESPONSE_OK.as_bytes())?;
            add_header(
                &mut stream,
                ("Content-Length", file_buffer.len().to_string().as_str()),
            )?;
            add_header(&mut stream, ("Content-Type", "application/octet-stream"))?;
            stream.write_all(EOL.as_bytes())?;
            stream.write_all(&file_buffer)?;
        }
        ("POST", _) if req.path.starts_with("/files/") => {
            let filename = &req.path[7..];
            if filename.is_empty() {
                stream.write_all(RESPONSE_BAD_REQUEST.as_bytes())?;
                return Ok(());
            }

            let filepath = format!("{}/{}", directory, filename);
            let mut file = std::fs::File::create(filepath)?;
            file.write_all(&req.body)?;
            file.flush()?;

            stream.write_all(RESPONSE_CREATED.as_bytes())?;
            stream.write_all(EOL.as_bytes())?;
        }
        _ if req.path.starts_with("/echo/") => {
            let message = &req.path[6..];
            let header_map = req.header_map();
            let accept_encoding = header_map.get("Accept-Encoding");

            stream.write_all(RESPONSE_OK.as_bytes())?;

            match accept_encoding {
                Some(encoding) => match encoding.contains(&"gzip".to_string()) {
                    true => {
                        add_header(&mut stream, ("Content-Encoding", "gzip"))?;
                        // compress the message in gzip
                        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                        encoder.write_all(message.as_bytes())?;
                        let compressed_bytes = encoder.finish()?;
                        add_header(&mut stream, ("Content-Type", "text/plain"))?;
                        write_body_bytes(&mut stream, &compressed_bytes)?;
                        return Ok(());
                    }
                    _ => {}
                },
                _ => { /* nothing */ }
            }

            add_header(&mut stream, ("Content-Type", "text/plain"))?;
            write_body(&mut stream, message)?;
            stream.write_all(message.as_bytes())?;
        }
        _ => {
            stream.write_all(RESPONSE_NOT_FOUND.as_bytes())?;
            stream.write_all(EOL.as_bytes())?;
        }
    }

    Ok(())
}

#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    headers: Vec<Header>,
    body: Vec<u8>,
}

type Header = (String, String);

struct HeaderMap {
    headers: HashMap<String, Vec<String>>,
}

impl HeaderMap {
    fn get(&self, key: &str) -> Option<Vec<String>> {
        self.headers.get(&key.to_lowercase()).cloned()
    }
}

impl Request {
    fn header_map(&self) -> HeaderMap {
        // split the value to array by ","
        HeaderMap {
            headers: self
                .headers
                .iter()
                .map(|(key, value)| {
                    (
                        key.to_lowercase(),
                        value.split(",").map(|s| s.trim().to_string()).collect(),
                    )
                })
                .collect(),
        }
    }
}

fn parse_request(stream: &TcpStream) -> Result<Request> {
    let mut buf_reader = BufReader::new(stream);
    let mut first_line = String::new();
    buf_reader.read_line(&mut first_line)?;
    let (method, path, _) = parse_first_line(&first_line).unwrap();
    let mut headers = Vec::new();
    let mut content_length_exists = false;
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        buf_reader.read_line(&mut line)?;
        if line == "\r\n" {
            break;
        }
        let mut parts = line.split(":");
        let key = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("Invalid header"))?
            .trim();
        let value = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("Invalid header"))?
            .trim();
        if key == "Content-Length" {
            content_length_exists = true;
            content_length = value.parse()?;
        }
        headers.push((key.to_string(), value.to_string()));
    }

    let mut body: Vec<u8>;
    if content_length_exists {
        body = vec![0; content_length];
        let n = buf_reader.read(&mut body)?;
        match n != content_length {
            true => return Err(anyhow::anyhow!("Invalid body")),
            false => {}
        }
    } else {
        body = Vec::new();
    }
    Ok(Request {
        method: method.to_string(),
        path: path.to_string(),
        headers,
        body,
    })
}

fn add_header(stream: &mut TcpStream, header: (&str, &str)) -> Result<()> {
    stream.write_fmt(format_args!("{}: {}\r\n", header.0, header.1))?;
    Ok(())
}

// in this function, it ends the headers
fn write_body(stream: &mut TcpStream, body: &str) -> Result<()> {
    add_header(stream, ("Content-Length", body.len().to_string().as_str()))?;
    stream.write_all(EOL.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    Ok(())
}

// in this function, it ends the headers
fn write_body_bytes(stream: &mut TcpStream, body: &Vec<u8>) -> Result<()> {
    add_header(stream, ("Content-Length", body.len().to_string().as_str()))?;
    stream.write_all(EOL.as_bytes())?;
    stream.write_all(body)?;
    Ok(())
}

fn parse_first_line(first_line: &str) -> Option<(&str, &str, &str)> {
    let mut parts = first_line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    let version = parts.next()?;
    Some((method, path, version))
}
