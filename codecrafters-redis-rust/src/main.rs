use core::result::Result::Ok;
use std::fmt::Write;
use std::path::PathBuf;
use std::time::Duration;
use std::vec;
use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Error, Result};
use base64::{prelude::*, write};
use bytes::{BufMut, BytesMut};
use chrono::prelude::*;
use clap::Parser;
use redis_starter_rust::command::{self, Command};
use redis_starter_rust::common::{RESPONSE_OK, RESPONSE_PONG};
use redis_starter_rust::id;
use redis_starter_rust::role::Role;
use redis_starter_rust::writer::Writer;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::task::spawn_local;
use tokio::time::{sleep, timeout, Instant};
use tokio::{join, spawn, stream, try_join};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
    sync::Mutex,
};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long, default_value = "6379")]
    port: u16,
    #[arg(short, long, default_value = "")]
    replicaof: String,
    #[arg(short, long, default_value = "/tmp")]
    dir: String,
    #[arg(short, long, default_value = "dump.rdb")]
    dbfilename: String,
}

struct RedisServer {
    id: String,
    role: Role,
    listener: TcpListener,
    sync_sender: broadcast::Sender<(i64, Command)>,
    replicas: Arc<Mutex<HashMap<Uuid, Replica>>>,
    config: HashMap<String, String>,
}

impl RedisServer {
    async fn load_rdb(&self, path: &PathBuf, storage: Arc<Storage>) -> Result<()> {
        let mut file = File::open(path).await?;
        let mut magic = [0u8; 9];
        let mut reader = BufReader::new(&mut file);
        reader.read_exact(&mut magic).await?;
        assert_eq!(magic[0..5], "REDIS".as_bytes().to_owned());
        println!("{}", String::from_utf8_lossy(magic.as_slice()));
        // hex: FA
        let mut auxiliary = [0u8; 1];
        reader.read_exact(&mut auxiliary).await?;
        assert_eq!(auxiliary[0], 0xFA);
        let mut metadata = Vec::new();
        reader.read_until(0xFE, &mut metadata).await?;
        println!("metadata: {:?}", String::from_utf8_lossy(&metadata));
        reader.consume(1); // skip db index
        reader.consume(1); // skip FB
        reader.consume(2); // skip two table size

        loop {
            // read data
            let mut data_type = [0u8; 1];
            reader.read_exact(&mut data_type).await?;
            if data_type[0] == 0xFF {
                // end of file
                break;
            }

            let mut expire_at = 0;
            if data_type[0] == 0xFC {
                let millis = reader.read_i64_le().await?;
                expire_at = DateTime::from_timestamp_millis(millis).unwrap().timestamp();
            } else if data_type[0] == 0xFD {
                let sec_timestamps = reader.read_i32_le().await?;
                expire_at = DateTime::from_timestamp(sec_timestamps as i64, 0)
                    .unwrap()
                    .timestamp();
            }
            if expire_at != 0 {
                data_type = [0u8; 1];
                reader.read_exact(&mut data_type).await?;
            }

            // check https://rdb.fnordig.de/file_format.html#value-type
            if data_type[0] != 0 {
                panic!("not supported");
            }
            let key_len = reader.read_u8().await?;
            let mut key = vec![0u8; key_len as usize];
            reader.read_exact(&mut key).await?;
            let value_len = reader.read_u8().await?;
            let mut value = vec![0u8; value_len as usize];
            reader.read_exact(&mut value).await?;

            storage.kv.lock().await.insert(
                String::from_utf8_lossy(&key).to_string(),
                Expirable { value, expire_at },
            );
        }

        Ok(())
    }
}

struct Replica {
    sent_offset: i64,
    data_offset: i64,
    ack_offset: i64,
}

impl Replica {
    fn synced(&self) -> bool {
        self.ack_offset >= self.data_offset
    }

    fn need_sync(&self) -> bool {
        println!(
            "data_offset: {}, ack_offset: {}",
            self.data_offset, self.ack_offset
        );
        self.data_offset > self.ack_offset
    }

    fn sent_data(&mut self, len: i64) {
        self.sent_offset += len;
        self.data_offset = self.sent_offset;
    }
}

impl RedisServer {
    pub fn info(&self) -> String {
        let mut info = "# Replication\nrole:".to_string();
        info.push_str(&self.role.to_string());
        info.push_str("\nmaster_replid:");
        info.push_str(&self.id);
        info.push_str("\nmaster_repl_offset:0");
        info
    }

    async fn get_synced_replicas_count(&self) -> u64 {
        let replicas = self.replicas.lock().await;
        let count = replicas.iter().filter(|(_, r)| r.synced()).count() as u64;
        count
    }
}

struct Storage {
    kv: Mutex<HashMap<String, Expirable<Vec<u8>>>>,
    streams: Mutex<HashMap<String, Entities>>,
    stream_waiters:
        Mutex<HashMap<String /* stream key */, broadcast::Sender<(String, StreamEntity)>>>,
}

fn write_stream_range_to_buf(entities: Vec<StreamEntity>, buf: &mut BytesMut) -> Result<()> {
    buf.write_str(format!("*{}\r\n", entities.len()).as_str())?;
    for s in entities.iter() {
        let entity_id = format!("{}-{}", s.entity_id.0, s.entity_id.1);

        buf.write_str(format!("*2\r\n").as_str())?;
        buf.write_str(format!("${}\r\n{}\r\n", entity_id.len(), entity_id).as_str())?;
        buf.write_str(format!("*{}\r\n", s.properties.len() * 2).as_str())?;
        for p in s.properties.iter() {
            buf.write_str(format!("${}\r\n{}\r\n", p.key.len(), p.key).as_str())?;
            buf.write_str(
                format!(
                    "${}\r\n{}\r\n",
                    p.value.len(),
                    String::from_utf8_lossy(&p.value)
                )
                .as_str(),
            )?;
        }
    }
    Ok(())
}

impl Storage {
    async fn read_stream_range(
        &self,
        key: String,
        start: String,
        end: String,
        contains_start: bool,
    ) -> Result<Vec<StreamEntity>> {
        let streams = self.streams.lock().await;

        if !streams.contains_key(&key) {
            return Ok(vec![]);
        }

        if start == "$" {
            return Ok(vec![]);
        }

        let streams = streams.get(&key).unwrap();
        let infinite_start = start == "-";
        let in_start: Comparator; // >=
        if infinite_start {
            in_start = Box::new(|_| true);
        } else {
            let tmp: Vec<&str> = start.split("-").collect();
            let l = tmp[0].parse::<i64>().unwrap();
            let r = tmp[1].parse::<i64>().unwrap();

            in_start = Box::new(move |entity_id: &EntityId| {
                if entity_id.0 > l {
                    return true;
                }
                if entity_id.0 == l && ((entity_id.1 >= r && contains_start) || entity_id.1 > r) {
                    return true;
                }
                false
            });
        }

        let infinite_end = end == "+";
        let in_end: Comparator; // <=
        if infinite_end {
            in_end = Box::new(|_| true);
        } else {
            let tmp: Vec<&str> = end.split("-").collect();
            let l = tmp[0].parse::<i64>().unwrap();
            let r = tmp[1].parse::<i64>().unwrap();

            in_end = Box::new(move |entity_id: &EntityId| {
                if entity_id.0 < l {
                    return true;
                }
                if entity_id.0 == l && entity_id.1 <= r {
                    return true;
                }
                false
            });
        }

        let mut entities: Vec<StreamEntity> = Vec::new();
        let mut start_index: i64 = streams.len() as i64;
        for i in 0..streams.len() {
            let s = streams.get(i).unwrap();
            if in_start(&s.entity_id) {
                start_index = i as i64;
                break;
            }
        }

        for i in start_index..streams.len() as i64 {
            let s = streams.get(i as usize).unwrap();
            if !in_end(&s.entity_id) {
                break;
            }
            entities.push(StreamEntity {
                entity_id: s.entity_id.clone(),
                properties: s.properties.iter().map(|p| p.clone()).collect(),
            });
        }

        Ok(entities)
    }
}

type Entities = Vec<StreamEntity>;

#[derive(Debug, Clone)]
struct StreamEntity {
    entity_id: EntityId,
    properties: Vec<Property>,
}

type Comparator = Box<dyn Fn(&EntityId) -> bool + Send>;

#[derive(Debug, Clone)]
struct EntityId(i64, i64);

impl EntityId {
    fn less_than(&self, other: &EntityId) -> bool {
        if self.0 < other.0 {
            return true;
        }
        if self.0 == other.0 && self.1 < other.1 {
            return true;
        }
        false
    }

    fn greater_than_str(&self, other: &str) -> bool {
        if other == "$" {
            return true;
        }

        let tmp: Vec<&str> = other.split("-").collect();
        let l = tmp[0].parse::<i64>().unwrap();
        let r = tmp[1].parse::<i64>().unwrap();
        if self.0 > l {
            return true;
        }
        if self.0 == l && self.1 > r {
            return true;
        }
        false
    }
}

#[derive(Debug, Clone)]
struct Property {
    key: String,
    value: Vec<u8>,
}

struct Expirable<T> {
    value: T,
    expire_at: i64, // 0 means no expiration
}

impl<T> Expirable<T> {
    fn expired(&self) -> bool {
        self.expire_at != 0 && self.expire_at < Utc::now().timestamp()
    }
}

impl RedisServer {
    async fn start_sync_replication(&self, stream: TcpStream) -> Result<()> {
        let mut sync_receiver = self.sync_sender.subscribe();
        let replicas = Arc::clone(&self.replicas);
        let replica_id = Uuid::new_v4();
        let replica = Replica {
            ack_offset: 0,
            sent_offset: 0,
            data_offset: 0,
        };
        {
            replicas.lock().await.insert(replica_id, replica);
        }

        let (mut ro, mut wo) = stream.into_split();

        let r_handler: tokio::task::JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            // only receive ACK
            let mut reader = BufReader::new(&mut ro);

            loop {
                let mut readn = 0;
                let args_count = command::read_arg_len(&mut readn, b'*', &mut reader).await?;
                let command =
                    command::parse_command_with_n(&mut readn, &mut reader, args_count).await?;
                match command {
                    Command::REPLCONF(args) => match (args.get(0), args.get(1)) {
                        (Some(op), Some(offset)) => {
                            if op == b"ACK" {
                                let offset =
                                    String::from_utf8_lossy(offset).parse::<i64>().unwrap();

                                println!("received ACK: {}", offset);
                                {
                                    replicas
                                        .lock()
                                        .await
                                        .get_mut(&replica_id)
                                        .unwrap()
                                        .ack_offset = offset;
                                } // unlock
                            }
                        }
                        _ => {
                            println!("only receive ACK command");
                        }
                    },
                    _ => {
                        println!("should not receive {:?} from the sync connection", command);
                    }
                }
            }
        });

        let replicas2 = Arc::clone(&self.replicas);
        let w_handler: tokio::task::JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            loop {
                let mut writer = Writer::new(&mut wo);
                match timeout(std::time::Duration::from_secs(5), sync_receiver.recv()).await {
                    Ok(Ok(cmd)) => match writer.write_command(&cmd.1).await {
                        Ok(_) => {
                            replicas2
                                .lock()
                                .await
                                .get_mut(&replica_id)
                                .unwrap()
                                .sent_data(cmd.0);
                            println!("Synced command to {:?}: {:?}", wo.peer_addr(), cmd.1);
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    },
                    Ok(Err(e)) => {
                        println!("Sync receiver error: {}", e);
                    }
                    Err(_e) => {
                        let need_sync: bool;
                        {
                            need_sync =
                                replicas2.lock().await.get(&replica_id).unwrap().need_sync();
                        }

                        if need_sync {
                            writer.write_array(&[b"REPLCONF", b"GETACK", b"*"]).await?;
                            replicas2
                                .lock()
                                .await
                                .get_mut(&replica_id)
                                .unwrap()
                                .sent_offset += 37;
                        }
                    }
                }
            }
        });

        let _result = tokio::join!(r_handler, w_handler);
        let replicas = Arc::clone(&self.replicas);
        replicas.lock().await.remove(&replica_id);

        Ok(())
    }

    async fn run(self, storage: Arc<Storage>) {
        let this = Arc::new(self);
        loop {
            let this = Arc::clone(&this);
            let stream = this.listener.accept().await;
            let storage = Arc::clone(&storage);
            match stream {
                Ok((mut stream, _)) => {
                    println!(
                        "Connection established: {:?}->{:?}",
                        stream.peer_addr(),
                        stream.local_addr()
                    );
                    tokio::spawn(async move {
                        match this.handle_connection(&mut stream, &storage).await {
                            Ok(_) => {
                                println!("Connection closed");
                            }
                            Err(err) => {
                                if err.to_string() == "PSYNC" {
                                    println!("Start sync replication");
                                    match this.start_sync_replication(stream).await {
                                        Ok(_) => {
                                            println!("Sync replication finished");
                                        }
                                        Err(e) => {
                                            println!("Sync replication error: {}", e);
                                        }
                                    }
                                } else {
                                    println!("error: {}", err);
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }

    async fn handle_connection(&self, stream: &mut TcpStream, storage: &Storage) -> Result<()> {
        loop {
            let cmd = command::parse_command(stream).await?;

            let command = cmd.1;
            self.handle_command(&command, stream, storage).await?;

            let is_write_command = command.is_write();
            if is_write_command {
                if self.sync_sender.receiver_count() == 0 {
                    continue;
                }
                match self.sync_sender.send((cmd.0 as i64, command.clone())) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Warning: {}", e);
                    }
                }
            }
        }
    }

    async fn handle_command(
        &self,
        cmd: &command::Command,
        stream: &mut TcpStream,
        storage: &Storage,
    ) -> Result<(), Error> {
        let mut writer = Writer::new(stream);

        if cmd.is_write() {
            if handle_write_command(&mut writer, cmd, storage).await? {
                return anyhow::Ok(());
            }
            // if it doesn't handle, let it go to the next step
        }

        match cmd {
            Command::PING => {
                writer.pong().await?;
            }
            Command::WAIT(wait_count, timeout_mill) => {
                let timeout = std::time::Duration::from_millis(*timeout_mill);
                let sleep_duration = std::time::Duration::from_millis(*timeout_mill / 10);

                let start = Instant::now();
                let mut synced_replicas_count = 0;
                loop {
                    let count = self.get_synced_replicas_count().await;
                    synced_replicas_count = count;
                    if count >= *wait_count {
                        break;
                    }
                    if start.elapsed() >= timeout {
                        break;
                    }
                    // sleep
                    sleep(sleep_duration).await;
                }
                self.get_synced_replicas_count().await;

                writer.write_integrate(synced_replicas_count as i64).await?;
            }
            Command::ECHO(data) => {
                writer
                    .write_bulk_string(String::from_utf8_lossy(data).as_bytes())
                    .await?;
            }
            Command::KEYS(pattern) => {
                let regex = regex::Regex::new(&pattern.replace("*", ".*"))?;

                let kv = storage.kv.lock().await;
                let keys = kv.keys();
                // convert pattern to regex
                let keys = keys.filter(|k| regex.is_match(&k));
                writer
                    .write_array(&keys.map(|k| k.as_bytes()).collect::<Vec<_>>())
                    .await?;
            }
            Command::CONFIG(config) => match config.op {
                command::ConfigOp::Get => {
                    let key = String::from_utf8_lossy(config.args.get(0).unwrap()).to_string();
                    if let Some(v) = self.config.get(&key) {
                        writer.write_array(&[key.as_bytes(), v.as_bytes()]).await?;
                    } else {
                        writer.write_array(&[]).await?;
                    }
                }
                command::ConfigOp::Set => {
                    panic!("not implemented");
                }
            },
            Command::GET(key) => match storage.kv.lock().await.get(key) {
                Some(entity) => match entity.expired() {
                    true => {
                        println!("{} expired1", key);
                        storage.kv.lock().await.remove(key);
                        writer.not_found().await?;
                    }
                    false => {
                        println!("{} exists", key);
                        writer
                            .write_bulk_string(String::from_utf8_lossy(&entity.value).as_bytes())
                            .await?;
                    }
                },
                None => {
                    println!("{} expired2", key);
                    writer.not_found().await?;
                }
            },
            Command::INFO => {
                writer.write_bulk_string(self.info().as_bytes()).await?;
            }
            Command::REPLCONF(_arg) => {
                writer.ok().await?;
            }
            Command::PSYNC(_, _) => {
                writer
                    .write_simple_string(format!("FULLRESYNC {} 0", self.id).as_bytes())
                    .await?;
                // start to sync RDB file
                // we use the fixed data https://github.com/codecrafters-io/redis-tester/blob/main/internal/assets/empty_rdb_hex.md
                let rdb = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
                let binary = BASE64_STANDARD.decode(rdb.as_bytes())?;
                writer.write_rdb(&binary).await?;
                return Err(anyhow!("PSYNC"));
            }
            Command::Type(key) => {
                let streams = storage.streams.lock().await;
                if streams.contains_key(key) {
                    writer.write_simple_string("stream".as_bytes()).await?;
                }
                let kv = storage.kv.lock().await;
                match kv.get(key) {
                    Some(entity) => {
                        if entity.expired() {
                            storage.kv.lock().await.remove(key);
                            writer.write_simple_string("none".as_bytes()).await?;
                        } else {
                            writer.write_simple_string("string".as_bytes()).await?;
                        }
                    }
                    None => {
                        writer.write_simple_string("none".as_bytes()).await?;
                    }
                }
            }
            Command::XRANGE(xrange) => {
                if !storage.streams.lock().await.contains_key(&xrange.key) {
                    writer.write_array(&[]).await?;
                    return Ok(());
                }

                let streams = storage
                    .read_stream_range(
                        xrange.key.clone(),
                        xrange.start.clone(),
                        xrange.end.clone(),
                        true,
                    )
                    .await?;
                let mut buf = BytesMut::new();
                write_stream_range_to_buf(streams, &mut buf)?;

                writer.write_raw(&buf).await?;
            }
            Command::XREAD(xread) => {
                let mut streams = vec![];

                for read_stream in xread.streams.iter() {
                    let result = storage
                        .read_stream_range(
                            read_stream.0.clone(),
                            read_stream.1.clone(),
                            "+".to_string(),
                            false,
                        )
                        .await?;
                    if result.len() != 0 {
                        streams.push((read_stream.0.clone(), result));
                    }
                }

                if streams.len() == 0 && xread.is_blocking {
                    // block until there is a new message
                    let mut receivers = vec![];
                    let mut waiters = storage.stream_waiters.lock().await;

                    let stream_key_map = xread
                        .streams
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<HashMap<_, _>>();

                    for read_stream in xread.streams.iter() {
                        if !waiters.contains_key(&read_stream.0) {
                            let (sender, receiver) = broadcast::channel(1024);
                            waiters.insert(read_stream.0.clone(), sender);
                            receivers.push(receiver);
                        } else {
                            receivers.push(waiters.get(&read_stream.0).unwrap().subscribe());
                        }
                    }
                    let mut futures = receivers.iter_mut().map(|r| r.recv()).collect::<Vec<_>>();
                    println!("waiters: {:?}", waiters.keys().collect::<Vec<_>>());
                    drop(waiters);

                    let no_timeout = xread.blocking_millis == 0;
                    let timeout_duration = Duration::from_millis(xread.blocking_millis);
                    loop {
                        tokio::select! {
                            result = futures.pop().unwrap(), if !futures.is_empty() => {
                                match result {
                                    Ok(entity) => {
                                        println!("Received entity: {:?}", entity.1);
                                        let stream_key = entity.0;
                                        let wait_id = stream_key_map.get(&stream_key).unwrap();
                                        if entity.1.entity_id.greater_than_str(&wait_id) {
                                            streams.push((stream_key.clone(), vec![entity.1]));
                                            break;
                                        }
                                    }
                                    Err(err) => println!("Error: {:?}", err)
                                }
                            }
                            _ = tokio::time::sleep(timeout_duration), if !no_timeout => {
                                break;
                            }
                        }
                    }
                }

                let mut buf = BytesMut::new();
                buf.write_str(format!("*{}\r\n", streams.len()).as_str())?;
                for s in streams.into_iter() {
                    buf.write_str("*2\r\n")?;
                    let stream_key = format!("${}\r\n{}\r\n", s.0.len(), s.0);
                    buf.write_str(stream_key.as_str())?;
                    write_stream_range_to_buf(s.1, &mut buf)?;
                }
                writer.write_raw(&buf).await?;
            }
            Command::INVALID(invalid_op) => match invalid_op {
                Some(op) => {
                    writer.command_not_found(op).await?;
                }
                None => {
                    writer.write_err("empty command").await?;
                }
            },
            Command::Error(msg) => {
                writer.write_simple_string(msg.as_bytes()).await?;
            }
            _ => panic!("can't arrive here"),
        }

        Ok(())
    }
}

#[warn(dead_code)]
async fn handle_ack_command<U>(writer: &mut Writer<U>, cmd: &Command, offset: i64) -> Result<bool>
where
    U: AsyncWriteExt + Unpin,
{
    match cmd {
        Command::REPLCONF(args) => {
            for i in (0..args.len()).step_by(2) {
                if args[i].to_ascii_uppercase() == b"GETACK" && args[i + 1] == b"*" {
                    writer
                        .write_array(&[b"REPLCONF", b"ACK", offset.to_string().as_bytes()])
                        .await?;
                }
                println!("{}", offset);
            }
            Ok(true)
        }
        Command::PING => Ok(true),
        _ => Ok(false),
    }
}

async fn handle_write_command<U>(
    writer: &mut Writer<U>,
    cmd: &Command,
    storage: &Storage,
) -> Result<bool>
where
    U: AsyncWriteExt + Unpin,
{
    match cmd {
        Command::SET(payload) => {
            if storage.streams.lock().await.contains_key(&payload.key) {
                return Err(anyhow!("Stream already exists, use XADD instead"));
            }

            let mut entity_value = Expirable {
                value: payload.value.clone(),
                expire_at: 0,
            };
            match (payload.expire_precision, payload.expire_value) {
                (Some(precision), Some(value)) => {
                    let expire_at = Utc::now();
                    let expire_at = match precision {
                        command::TimePrecision::Seconds => {
                            expire_at + chrono::Duration::seconds(value as i64)
                        }
                        command::TimePrecision::MilliSeconds => {
                            expire_at + chrono::Duration::milliseconds(value as i64)
                        }
                    };
                    entity_value.expire_at = expire_at.timestamp();
                }
                _ => {}
            }
            storage
                .kv
                .lock()
                .await
                .insert(payload.key.clone(), entity_value);
            writer.ok().await?;
            Ok(true)
        }
        Command::XADD(xadd) => {
            if storage.kv.lock().await.contains_key(&xadd.key) {
                return Err(anyhow!("Key already exists, use SET instead"));
            }

            let properties = xadd
                .items
                .iter()
                .map(|(k, v)| Property {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect();

            let millis;
            let mut sequence_number = 0;
            let auto_increment;
            if xadd.entity_id == "*" {
                millis = Utc::now().timestamp_millis();
                auto_increment = true;
            } else {
                let entity_id: Vec<&str> = xadd.entity_id.split('-').collect();
                let millis2 = entity_id.get(0).unwrap().parse::<i64>();
                let sequence = entity_id.get(1);
                if millis2.is_err() || sequence.is_none() {
                    writer
                        .write_simple_string(
                            "(error) ERR Invalid stream ID specified as stream command argument"
                                .as_bytes(),
                        )
                        .await?;
                    return Ok(true);
                }
                millis = millis2.unwrap();
                let try_sequence_number = sequence.unwrap().parse::<i64>();
                if try_sequence_number.is_err() && sequence.unwrap().to_owned() != "*" {
                    writer
                        .write_simple_string(
                            "(error) ERR Invalid stream ID specified as stream command argument"
                                .as_bytes(),
                        )
                        .await?;
                    return Ok(true);
                }
                auto_increment = try_sequence_number.is_err(); // if sequence is *, auto increment
                if auto_increment {
                    if millis == 0 {
                        sequence_number = 1; // no 0-0
                    } else {
                        sequence_number = 0;
                    }
                } else {
                    sequence_number = try_sequence_number.unwrap();
                }
            }

            if millis == 0 && sequence_number == 0 {
                writer
                    .write_simple_string(
                        "(error) ERR The ID specified in XADD must be greater than 0-0".as_bytes(),
                    )
                    .await?;
                return Ok(true);
            }

            let mut streams = storage.streams.lock().await;
            let entity;
            if !streams.contains_key(&xadd.key) {
                entity = StreamEntity {
                    entity_id: EntityId(millis, sequence_number),
                    properties,
                };
                streams.insert(xadd.key.clone(), vec![entity.clone()]);
            } else {
                let entities = streams.get_mut(&xadd.key).unwrap();
                let previous = entities.last().unwrap();
                if auto_increment && previous.entity_id.0 == millis {
                    sequence_number = previous.entity_id.1 + 1;
                }

                entity = StreamEntity {
                    entity_id: EntityId(millis, sequence_number),
                    properties,
                };

                if !previous.entity_id.less_than(&entity.entity_id) {
                    writer.write_simple_string("(error) ERR The ID specified in XADD is equal or smaller than the target stream top item".as_bytes()).await?;
                    return Ok(true);
                }
                entities.push(entity.clone());
            }

            let mut waiters = storage.stream_waiters.lock().await;
            if let Some(sender) = waiters.get(&xadd.key) {
                if sender.receiver_count() == 0 {
                    waiters.remove(&xadd.key);
                } else {
                    sender.send((xadd.key.clone(), entity))?;
                }
            }

            let entity_id = format!("{}-{}", millis, sequence_number);
            writer.write_bulk_string(entity_id.as_bytes()).await?;

            Ok(true)
        }
        _ => Ok(false),
    }
}

async fn handle_replication_stream(stream: TcpStream, storage: Arc<Storage>) -> Result<()> {
    let mut sync_offset = 0;
    let (mut ro, mut wo) = stream.into_split();
    let mut reader = BufReader::new(&mut ro);
    let mut writer = Writer::new(&mut wo);
    loop {
        let mut prefix = [0u8; 1];
        match reader.read_exact(&mut prefix).await {
            Err(_) => return Ok(()),
            Ok(_) => {}
        };
        match prefix[0] {
            b'$' => {
                // simple string without CRLF
                let mut buf = Vec::new();
                reader.read_until(b'\n', &mut buf).await?;
                let len = String::from_utf8_lossy(&buf)
                    .trim()
                    .parse::<usize>()
                    .unwrap();
                let mut rdb = vec![0u8; len];
                reader.read_exact(&mut rdb).await?;
                // do nothing
                println!("RDB: {:?}", rdb);
            }
            b'*' => {
                let mut buf = String::new();
                reader.read_line(&mut buf).await?;
                if buf.len() == 0 {
                    return Err(anyhow::anyhow!("Invalid number"));
                }
                let mut readn = 1 + buf.len();
                let num = buf.trim().parse::<u32>()?;
                let cmd = command::parse_command_with_n(&mut readn, &mut reader, num).await?;
                let old_sync_offset = sync_offset;
                sync_offset += readn as i64;

                println!("replication command: {:?}", cmd);

                if handle_ack_command(&mut writer, &cmd, old_sync_offset).await? {
                    continue;
                }
                if !handle_write_command(&mut writer, &cmd, &storage).await? {
                    panic!("couldn't handle the command: {:?}", cmd);
                }
            }
            _ => {
                println!("Replication stream: {:?}", prefix);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!("Listening on port {}", args.port);
    let addr = format!("127.0.0.1:{}", args.port);
    let listener = TcpListener::bind(addr).await.unwrap();

    let mut role = Role::Master;
    let mut sync_conn = None;
    if args.replicaof != "" {
        role = Role::Slave;
        // send a PING to the master
        let addrs: Vec<&str> = args.replicaof.split(" ").collect();
        let host = addrs.get(0).unwrap().to_string();
        let port = addrs.get(1).unwrap().parse::<u16>().unwrap();

        println!("Connecting to master at {}:{}", host, port);
        // PING
        let mut conn = TcpStream::connect(format!("{}:{}", &host, port)).await?;
        let (mut r, w) = tokio::io::split(&mut conn);
        let mut writer = Writer::new(w);

        writer.write_array(&[b"PING"]).await?;
        let mut pong = Vec::with_capacity(RESPONSE_PONG.len());
        r.read_buf(&mut pong).await?;
        assert_eq!(pong, RESPONSE_PONG);

        // REPLCONF listening-port <PORT>
        writer
            .write_array(&[
                b"REPLCONF",
                b"listening-port",
                args.port.to_string().as_bytes(),
            ])
            .await?;
        let mut ok = Vec::with_capacity(RESPONSE_OK.len());
        r.read_buf(&mut ok).await?;
        assert_eq!(ok, RESPONSE_OK);

        writer
            .write_array(&[b"REPLCONF", b"capa", b"eof", b"capa", b"psync2"])
            .await?;
        let mut ok = Vec::with_capacity(RESPONSE_OK.len());
        r.read_buf(&mut ok).await?;
        assert_eq!(ok, RESPONSE_OK);

        writer.write_array(&[b"PSYNC", b"?", b"-1"]).await?;

        let mut reader = BufReader::new(r);
        let mut buf = Vec::new();
        reader.read_until(b'\n', &mut buf).await?;
        assert_eq!(buf.starts_with(b"+FULLRESYNC"), true);
        let sync_reply = buf
            .split(|&x| x == b' ')
            .map(|x| String::from_utf8_lossy(x))
            .collect::<Vec<_>>();
        let _master_id = sync_reply.get(1).unwrap();
        let _sync_offset = sync_reply.get(2).unwrap().trim().parse::<u16>().unwrap();

        sync_conn = Some(conn);
    }

    let (sync_sender, _) = broadcast::channel(1024);

    let storage = Storage {
        kv: Mutex::new(HashMap::new()),
        streams: Mutex::new(HashMap::new()),
        stream_waiters: Mutex::new(HashMap::new()),
    };
    let mut config = HashMap::new();
    let mut rdb_path = PathBuf::from(&args.dir);
    rdb_path = rdb_path.join(&args.dbfilename);
    config.insert("dir".to_string(), args.dir);
    config.insert("dbfilename".to_string(), args.dbfilename);
    let sa = Arc::new(storage);
    let storage2 = Arc::clone(&sa);
    let server = RedisServer {
        id: id::generate_id(),
        role,
        listener,
        sync_sender,
        replicas: Arc::new(Mutex::new(HashMap::new())),
        config,
    };

    if let Some(conn) = sync_conn {
        println!(
            "conn is: {}, {}",
            conn.local_addr().unwrap(),
            conn.peer_addr().unwrap()
        );
        tokio::spawn(async move {
            match handle_replication_stream(conn, storage2).await {
                Ok(_) => {
                    println!("Replication stream finished");
                }
                Err(e) => {
                    println!("Replication stream error: {}", e);
                }
            }
        });
    }

    println!("rdb path: {:?}", rdb_path);
    if rdb_path.exists() {
        server.load_rdb(&rdb_path, Arc::clone(&sa)).await?;
    }

    server.run(Arc::clone(&sa)).await;
    Ok(())
}
