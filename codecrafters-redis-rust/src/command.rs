use anyhow::Ok;
use anyhow::Result;
use std::option::Option;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::BufReader;
use tokio::{io::AsyncReadExt, net::TcpStream};

#[derive(Debug, Clone)]
pub enum Command {
    Error(String),
    INVALID(Option<Vec<u8>>),
    PING,
    WAIT(u64, u64),
    ECHO(Vec<u8>),
    SET(Set),
    GET(String),
    INFO,
    REPLCONF(Vec<Vec<u8>>),
    PSYNC(String, i64),
    CONFIG(Config),
    KEYS(String),
    Type(String),
    XADD(XAdd),
    XRANGE(XRange),
    XREAD(XRead),
}

#[derive(Debug, Clone)]
pub struct SizeCommand(pub usize, pub Command);

impl Command {
    pub fn is_write(&self) -> bool {
        match self {
            Command::SET(_) => true,
            Command::XADD(_) => true,
            _ => false,
        }
    }

    pub fn args(&self) -> Vec<Vec<u8>> {
        match self {
            Command::SET(set) => {
                let mut args: Vec<Vec<u8>> = vec![
                    b"SET".to_vec(),
                    set.key.clone().into_bytes(),
                    set.value.clone(),
                ];
                if let Some(precision) = set.expire_precision {
                    args.push(match precision {
                        TimePrecision::Seconds => b"EX".to_vec(),
                        TimePrecision::MilliSeconds => b"PX".to_vec(),
                    });
                }
                if let Some(value) = set.expire_value {
                    args.push(value.to_string().as_bytes().to_vec());
                }
                args
            }
            _ => panic!("Not implemented"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct XAdd {
    pub key: String,
    pub entity_id: String,
    pub items: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct XRange {
    pub key: String,
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone)]
pub struct XRead {
    pub is_blocking: bool,
    pub blocking_millis: u64, // 0 for not timeout
    pub streams: Vec<(String /* stream_key */, String /* id */)>,
}

#[derive(Clone)]
pub struct Set {
    pub key: String,
    pub value: Vec<u8>,
    pub expire_precision: Option<TimePrecision>,
    pub expire_value: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub op: ConfigOp,
    pub args: Vec<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub enum ConfigOp {
    Set,
    Get,
}

impl std::fmt::Display for Set {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SET {} {}",
            self.key,
            String::from_utf8_lossy(&self.value)
        )?;
        if let Some(precision) = self.expire_precision {
            match precision {
                TimePrecision::Seconds => write!(f, " EX")?,
                TimePrecision::MilliSeconds => write!(f, " PX")?,
            }
        }
        if let Some(value) = self.expire_value {
            write!(f, " {}", value)?;
        }
        std::result::Result::Ok(())
    }
}

impl std::fmt::Debug for Set {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Set {{ key: {}, value: {}, expire_precision: {:?}, expire_value: {:?} }}",
            self.key,
            String::from_utf8_lossy(&self.value),
            self.expire_precision,
            self.expire_value
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TimePrecision {
    Seconds,
    MilliSeconds,
}

pub async fn parse_command_with_n<R>(
    readn: &mut usize,
    buf_reader: &mut BufReader<R>,
    args_count: u32,
) -> Result<Command>
where
    R: AsyncRead + Unpin,
{
    let mut args: Vec<Vec<u8>> = Vec::new();
    for _ in 0..args_count {
        let arg_len = read_arg_len(readn, b'$', buf_reader).await?;
        let arg = read_arg(readn, buf_reader, arg_len).await?;
        args.push(arg);
    }

    // println!("{:?}", args);
    // for arg in args.iter() {
    //     println!("{:?}", String::from_utf8_lossy(arg));
    // }

    if args.len() == 0 {
        return Ok(Command::INVALID(None));
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"PING" => Ok(Command::PING),
        b"WAIT" => {
            let timeout = String::from_utf8_lossy(args.get(1).unwrap())
                .parse::<u64>()
                .unwrap();
            let count = String::from_utf8_lossy(args.get(2).unwrap())
                .parse::<u64>()
                .unwrap();
            Ok(Command::WAIT(timeout, count))
        }
        b"CONFIG" => {
            let op = match args.get(1).unwrap().to_ascii_uppercase().as_slice() {
                b"SET" => ConfigOp::Set,
                b"GET" => ConfigOp::Get,
                _ => return Ok(Command::INVALID(Some(b"CONFIG".to_vec()))),
            };
            Ok(Command::CONFIG(Config {
                op,
                args: args.iter().skip(2).map(|arg| arg.clone()).collect(),
            }))
        }
        b"ECHO" => Ok(Command::ECHO(args.get(1).unwrap().clone())),
        b"SET" => {
            Ok(Command::SET(Set {
                key: args.get(1).unwrap().iter().map(|&c| c as char).collect(),
                value: args.get(2).unwrap().clone(),
                expire_precision: args
                    .get(3)
                    .map(|arg| match arg.to_ascii_uppercase().as_slice() {
                        b"EX" => Some(TimePrecision::Seconds),
                        b"PX" => Some(TimePrecision::MilliSeconds),
                        _ => None,
                    })
                    .flatten(),
                expire_value: args
                    .get(4)
                    .map(|arg| {
                        // convert to string, then parse to u64
                        arg.iter()
                            .map(|&c| c as char)
                            .collect::<String>()
                            .parse::<i64>()
                            .ok()
                    })
                    .flatten(),
            }))
        }
        b"GET" => Ok(Command::GET(
            args.get(1).unwrap().iter().map(|&c| c as char).collect(),
        )),
        b"INFO" => Ok(Command::INFO),
        b"REPLCONF" => {
            if args.len() % 2 != 1 {
                return Ok(Command::INVALID(Some(
                    "Invalid number of arguments for REPLCONF"
                        .as_bytes()
                        .to_vec(),
                )));
            }
            Ok(Command::REPLCONF(
                args.iter().skip(1).map(|arg| arg.clone()).collect(),
            ))
        }
        b"PSYNC" => Ok(Command::PSYNC(
            String::from_utf8_lossy(args.get(1).unwrap()).to_string(),
            String::from_utf8_lossy(args.get(2).unwrap())
                .parse::<i64>()
                .unwrap(),
        )),
        b"KEYS" => Ok(Command::KEYS(
            args.get(1).unwrap().iter().map(|&c| c as char).collect(),
        )),
        b"TYPE" => Ok(Command::Type(
            args.get(1).unwrap().iter().map(|&c| c as char).collect(),
        )),
        b"XADD" => {
            if args.len() < 5 || args.len() % 2 != 1 {
                return Ok(Command::INVALID(Some(
                    "Invalid number of arguments for XADD".as_bytes().to_vec(),
                )));
            }
            let key = String::from_utf8_lossy(args.get(1).unwrap()).to_string();
            let items: Vec<(String, Vec<u8>)>;
            items = args
                .iter()
                .skip(3)
                .step_by(2)
                .zip(args.iter().skip(4).step_by(2))
                .map(|(field, value)| (String::from_utf8_lossy(field).to_string(), value.clone()))
                .collect();
            Ok(Command::XADD(XAdd {
                key,
                entity_id: args.get(2).unwrap().iter().map(|&c| c as char).collect(),
                items,
            }))
        }
        b"XRANGE" => {
            if args.len() != 4 {
                return Ok(Command::INVALID(Some(
                    "Invalid number of arguments for XRANGE".as_bytes().to_vec(),
                )));
            }
            Ok(Command::XRANGE(XRange {
                key: String::from_utf8_lossy(args.get(1).unwrap()).to_string(),
                start: String::from_utf8_lossy(args.get(2).unwrap()).to_string(),
                end: String::from_utf8_lossy(args.get(3).unwrap()).to_string(),
            }))
        }
        b"XREAD" => {
            if args.len() < 4 {
                return Ok(Command::INVALID(Some(
                    "Invalid number of arguments for XSTREAM"
                        .as_bytes()
                        .to_vec(),
                )));
            }

            let mut skip = 1;
            let mut is_blocking = false;
            let mut blocking_millis = 0;
            if args.get(1).unwrap().to_ascii_lowercase() == b"block" {
                is_blocking = true;
                blocking_millis = String::from_utf8_lossy(args.get(2).unwrap())
                    .parse::<u64>()
                    .unwrap();
                skip += 2;
            }

            let args: Vec<Vec<u8>> = args.iter().skip(skip).map(|arg| arg.clone()).collect();

            if args.len() % 2 != 1 || args.get(0).unwrap().to_ascii_lowercase() != b"streams" {
                return Ok(Command::INVALID(Some(
                    "Invalid number of arguments for XSTREAM"
                        .as_bytes()
                        .to_vec(),
                )));
            }

            // from 1: k1 k2 k3 id1 id2 id3
            let streams: Vec<(String, String)> = args
                .iter()
                .skip(1)
                .take((args.len() - 1) / 2)
                .zip(args.iter().skip(1 + args.len() / 2))
                .map(|(stream_key, id)| {
                    (
                        String::from(String::from_utf8_lossy(stream_key)),
                        String::from(String::from_utf8_lossy(id)),
                    )
                })
                .collect();
            Ok(Command::XREAD(XRead {
                is_blocking,
                blocking_millis,
                streams,
            }))
        }
        _ => Ok(Command::INVALID(Some(args.get(0).unwrap().clone()))),
    }
}

pub async fn parse_command(stream: &mut TcpStream) -> Result<SizeCommand> {
    let mut buf_reader = BufReader::new(stream);
    let mut readn = 0;
    let args_count = read_arg_len(&mut readn, b'*', &mut buf_reader).await?;
    let command = parse_command_with_n(&mut readn, &mut buf_reader, args_count).await?;
    Ok(SizeCommand(readn, command))
}

pub async fn read_arg_len<R>(
    readn: &mut usize,
    prefix: u8,
    buf_reader: &mut BufReader<R>,
) -> Result<u32>
where
    R: AsyncRead + Unpin,
{
    let mut buf = String::new();
    buf_reader.read_line(&mut buf).await?;
    *readn += buf.as_bytes().len();
    if buf.len() < 2 || buf.as_bytes()[0] != prefix {
        return Err(anyhow::anyhow!("Invalid number"));
    }
    let num = buf[1..].trim().parse::<u32>()?;
    Ok(num)
}

async fn read_arg<R>(readn: &mut usize, buf_reader: &mut BufReader<R>, len: u32) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut buf = vec![0; len as usize];
    let read = buf_reader.read_exact(&mut buf).await?;
    if read != len as usize {
        return Err(anyhow::anyhow!("Failed to read string"));
    }
    *readn += read + 2; // 2 for CRLF
    buf_reader.consume(2);
    Ok(buf)
}
