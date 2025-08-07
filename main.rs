use tokio::{net::TcpListener, io::{AsyncBufReadExt, AsyncWriteExt, BufReader}};
use dashmap::DashMap;
use std::sync::Arc;

type Store = Arc<DashMap<String, String>>;

#[tokio::main]
async fn main() {
    let store: Store = Arc::new(DashMap::new());
    let listener = TcpListener::bind("0.0.0.0:6379").await.unwrap();

    println!("🔧 LightningKV listening on 6379");

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let store = store.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            loop {
                line.clear();
                let bytes = reader.read_line(&mut line).await.unwrap();
                if bytes == 0 {
                    break;
                }

                let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
                match parts.as_slice() {
                    ["SET", key, value] => {
                        store.insert(key.to_string(), value.to_string());
                        writer.write_all(b"+OK\n").await.unwrap();
                    }
                    ["GET", key] => {
                        match store.get(*key) {
                            Some(v) => {
                                writer.write_all(format!("${}\n{}\n", v.len(), v).as_bytes()).await.unwrap();
                            }
                            None => {
                                writer.write_all(b"$-1\n").await.unwrap();
                            }
                        }
                    }
                    ["DEL", key] => {
                        store.remove(*key);
                        writer.write_all(b"+OK\n").await.unwrap();
                    }
                    _ => {
                        writer.write_all(b"-ERR unknown command\n").await.unwrap();
                    }
                }
            }
        });
    }
}
