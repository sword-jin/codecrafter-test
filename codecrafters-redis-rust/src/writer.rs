use anyhow::Result;
use bytes::{BufMut, BytesMut};
use tokio::io::AsyncWriteExt;

use crate::{command, common};

pub struct Writer<U>(U);

impl<U: AsyncWriteExt + Unpin> Writer<U> {
    pub fn new(stream: U) -> Self {
        Self(stream)
    }

    pub async fn write_raw(&mut self, data: &[u8]) -> Result<()> {
        self.0.write_all(data).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn write_bulk_string(&mut self, data: &[u8]) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put(format!("${}\r\n", data.len()).as_bytes());
        buf.put(data);
        buf.put_slice(b"\r\n");

        self.0.write_all(&buf).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn write_rdb(&mut self, data: &[u8]) -> Result<()> {
        self.0
            .write_all(format!("${}\r\n", data.len()).as_bytes())
            .await?;
        self.0.flush().await?;

        self.0.write_all(data).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn write_ok(&mut self) -> Result<()> {
        self.0.write_all(common::RESPONSE_OK).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn write_command(&mut self, cmd: &command::Command) -> Result<()> {
        let args = cmd.args();
        let args: Vec<&[u8]> = args.iter().map(|arg| arg.as_slice()).collect();
        let args = args.as_slice();
        self.write_array(args).await?;
        Ok(())
    }

    pub async fn write_simple_string(&mut self, data: &[u8]) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put(format!("+{}\r\n", String::from_utf8_lossy(data)).as_bytes());

        self.0.write_all(&buf).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn write_integrate(&mut self, data: i64) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put(format!(":{}\r\n", data).as_bytes());

        self.0.write_all(&buf).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn write_array(&mut self, data: &[&[u8]]) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put(format!("*{}\r\n", data.len()).as_bytes());
        for d in data.iter() {
            buf.put(format!("${}\r\n", d.len()).as_bytes());
            buf.put_slice(d);
            buf.put_slice(b"\r\n");
        }

        self.0.write(&buf).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn pong(&mut self) -> Result<()> {
        self.0.write_all(common::RESPONSE_PONG).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn not_found(&mut self) -> Result<()> {
        self.0.write_all(common::RESPONSE_NOT_FOUND).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn ok(&mut self) -> Result<()> {
        self.0.write_all(common::RESPONSE_OK).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn write_err(&mut self, msg: &str) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put(format!("-Err {}\r\n", msg).as_bytes());

        self.0.write_all(&buf).await?;
        self.0.flush().await?;
        Ok(())
    }

    pub async fn command_not_found(&mut self, command: &Vec<u8>) -> Result<()> {
        self.write_err(&format!(
            "unknown command '{}'",
            String::from_utf8_lossy(&command)
        )).await?;
        Ok(())
    }
}
