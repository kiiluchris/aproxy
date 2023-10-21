use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use config::Config;
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::builder()
        .add_source(config::File::with_name("config.json"))
        .build()
        .context("failed to load config")?;
    let app_cfg: AppConfig = cfg
        .try_deserialize()
        .context("failed to deserialize app config")?;

    let mut set = tokio::task::JoinSet::new();
    for cfg in app_cfg.apps {
        set.spawn(init_group_listeners(cfg));
    }

    while let Some(res) = set.join_next().await {
        println!("{:?}", res);
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AppConfig {
    apps: Vec<GroupConfig>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GroupConfig {
    name: String,
    ports: Vec<u16>,
    targets: Vec<String>,
}

async fn init_group_listeners(cfg: GroupConfig) -> anyhow::Result<()> {
    let lb = Arc::new(Mutex::new(RoundRobinLb::new(cfg.targets)));

    let mut set = tokio::task::JoinSet::new();
    for p in cfg.ports.iter() {
        let lb = Arc::clone(&lb);
        set.spawn(init_group_listener(lb, cfg.name.to_owned(), *p));
    }

    while let Some(res) = set.join_next().await {
        println!("{:?}", res);
    }

    Ok(())
}

async fn init_group_listener(
    lb: Arc<Mutex<RoundRobinLb>>,
    app_name: String,
    p: u16,
) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], p));
    let listener = TcpListener::bind(&addr)
        .await
        .context("failed to bind app listener")?;

    loop {
        let (mut origin, _) = listener.accept().await?;
        println!("[{app_name}] {addr} received new request");

        let mut tgt = lb.lock().await;
        while let Some(t) = tgt.next() {
            match TcpStream::connect(&t).await {
                Ok(mut dest) => {
                    drop(tgt);

                    let app_name = app_name.to_owned();
                    tokio::spawn(async move {
                        println!("[{app_name}] {addr} routed request to {t}");

                        match tokio::time::timeout(
                            Duration::from_secs(20),
                            tokio::io::copy_bidirectional(&mut origin, &mut dest),
                        )
                        .await
                        {
                            Ok(Ok(_)) => println!("[{app_name}] {addr} request to {t} complete"),
                            Err(elapsed) => {
                                println!("[{app_name}] {addr} request to {t} timed out {elapsed:?}")
                            }
                            Ok(Err(err)) => {
                                println!("[{app_name}] {addr} request to {t} failed {err:?}")
                            }
                        }
                    });

                    break;
                }
                Err(err) => {
                    println!("[{app_name}] {addr} failed to connect to {t}: {err:?}");
                }
            }
        }
    }
}

struct RoundRobinLb {
    current: usize,
    targets: Vec<String>,
}

impl RoundRobinLb {
    pub fn new(targets: Vec<String>) -> RoundRobinLb {
        RoundRobinLb {
            current: 0,
            targets,
        }
    }
}

impl Iterator for RoundRobinLb {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.targets.is_empty() {
            return None;
        }

        if self.current >= self.targets.len() {
            self.current = 0;
        }

        let res = self.targets[self.current].to_owned();
        self.current += 1;
        Some(res)
    }
}
