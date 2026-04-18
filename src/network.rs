use crate::{common::Error, resp::RespValue};
use anyhow::Context;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    net::TcpStream,
};

pub(crate) trait Reader {
    async fn read_line(&mut self, buf: &mut String) -> Result<usize, Error>;
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize, Error>;
}

pub struct TcpStreamReader<'a> {
    buf_reader: BufReader<&'a mut TcpStream>,
}

impl<'a> TcpStreamReader<'a> {
    pub(crate) fn new(tcp_stream: &'a mut TcpStream) -> Self {
        Self {
            buf_reader: BufReader::new(tcp_stream),
        }
    }

    fn get_mut(&mut self) -> &mut TcpStream {
        self.buf_reader.get_mut()
    }
}

impl<'a> Reader for TcpStreamReader<'a> {
    async fn read_line(&mut self, buf: &mut String) -> Result<usize, Error> {
        self.buf_reader.read_line(buf).await.map_err(|e| e.into())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.buf_reader.read_exact(buf).await.map_err(|e| e.into())
    }
}

pub(crate) struct FileReader {
    buf_reader: BufReader<File>,
}

impl FileReader {
    fn new(file: File) -> Self {
        Self {
            buf_reader: BufReader::new(file),
        }
    }
}

impl Reader for FileReader {
    async fn read_line(&mut self, buf: &mut String) -> Result<usize, Error> {
        self.buf_reader.read_line(buf).await.map_err(|e| e.into())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.buf_reader.read_exact(buf).await.map_err(|e| e.into())
    }
}

pub(crate) struct StreamReader<R: Reader> {
    reader: R,
    uncommitted_byte_count: usize,
    pub(crate) byte_count: usize,
}

impl<'a> StreamReader<TcpStreamReader<'a>> {
    pub(crate) fn from_tcp_stream(stream: &'a mut TcpStream) -> StreamReader<TcpStreamReader<'a>> {
        Self::new(TcpStreamReader::new(stream))
    }

    pub(crate) fn get_mut(&mut self) -> &mut TcpStream {
        self.reader.get_mut()
    }
}

impl StreamReader<FileReader> {
    pub(crate) fn from_file(file: File) -> StreamReader<FileReader> {
        Self::new(FileReader::new(file))
    }
}

impl<R: Reader> StreamReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            uncommitted_byte_count: 0,
            byte_count: 0,
        }
    }

    pub(crate) fn reset_byte_counter(&mut self) {
        self.uncommitted_byte_count = 0;
        self.byte_count = 0;
    }

    pub(crate) fn commit_byte_count(&mut self) {
        self.byte_count = self.uncommitted_byte_count;
    }

    pub(crate) async fn read_resp_value_from_buf_reader(
        &mut self,
        request_count: Option<u64>,
    ) -> Result<Option<RespValue>, Error> {
        self.read_resp_value(request_count).await
    }

    async fn read_resp_value(
        &mut self,
        request_count: Option<u64>,
    ) -> Result<Option<RespValue>, Error> {
        let line = self.read_line_from_tcp_stream(request_count).await?;

        if line.is_empty() {
            return Ok(None);
        } else if line.starts_with("+") {
            return Ok(Some(RespValue::SimpleString(line[1..].trim().to_string())));
        } else if line.starts_with("$") {
            let bulk_str_len =
                usize::from_str_radix(&line[1..].trim(), 10).context("parse-bulk-str-len")?;

            let next_line = self.read_line_from_tcp_stream(request_count).await?;

            if next_line.trim().len() != bulk_str_len {
                return Err(format!(
                    "Bulk string len mismatch. Expected {}, got {}. Bulk string: {}",
                    bulk_str_len,
                    next_line.len(),
                    &next_line
                )
                .into());
            }

            return Ok(Some(RespValue::BulkString(next_line.trim().to_string())));
        } else if line.starts_with("*") {
            let array_len =
                usize::from_str_radix(&line[1..].trim(), 10).context("parse-array-len")?;
            let mut items = vec![];

            for _ in 0..array_len {
                match Box::pin(self.read_resp_value(request_count)).await? {
                    Some(item) => items.push(item),
                    None => return Err("Missing array item".into()),
                }
            }

            return Ok(Some(RespValue::Array(items)));
        } else if line.starts_with(":") {
            let v = i64::from_str_radix(&line[1..].trim(), 10).context("parse-array-len")?;
            return Ok(Some(RespValue::Integer(v)));
        } else if line.starts_with(",") {
            let v = line[1..].trim().parse::<f64>().expect("parsing-double");
            return Ok(Some(RespValue::Double(v)));
        }

        Err(format!("Unexpected incoming RESP string from connection: {}", line).into())
    }

    async fn read_line_from_tcp_stream(
        &mut self,
        request_count: Option<u64>,
    ) -> Result<String, Error> {
        let mut buf = String::new();
        self.reader.read_line(&mut buf).await?;
        self.uncommitted_byte_count += buf.len();

        debug!(
            "Incoming {} bytes from TcpStream [{}] (RC: {:?})",
            buf.len(),
            buf.trim_end(),
            request_count
        );

        Ok(buf)
    }

    pub(crate) async fn read_bulk_bytes_from_tcp_stream(
        &mut self,
        request_count: Option<u64>,
    ) -> Result<Vec<u8>, Error> {
        let mut size_raw = String::new();
        self.reader.read_line(&mut size_raw).await?;
        self.uncommitted_byte_count += size_raw.len();

        let size_raw = size_raw.trim_end();

        if !size_raw.starts_with("$") {
            let next = self.read_line_from_tcp_stream(request_count).await?;
            return Err(format!(
                "Invalid start of size for bulk bytes. Got: {} (Next: {}) (RC: {:?})",
                size_raw, next, request_count
            )
            .into());
        }

        let size = usize::from_str_radix(&size_raw[1..], 10).context("convert-size-to-usize")?;

        let mut buf = Vec::with_capacity(size);
        buf.resize(size, 0);
        let read_size = self.reader.read_exact(&mut buf[0..size]).await?;
        self.uncommitted_byte_count += read_size;

        if read_size != size {
            return Err(
                format!("Read size mismatch. Read {}. Expected {}.", read_size, size).into(),
            );
        }

        debug!(
            "Incoming {} bytes from TcpStream (RC: {:?})",
            buf.len(),
            request_count
        );

        Ok(buf)
    }
}
