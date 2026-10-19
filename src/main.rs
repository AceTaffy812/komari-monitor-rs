#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::similar_names,
    clippy::too_many_lines
)]

use crate::callbacks::handle_callbacks;
use crate::command_parser::Args;
use crate::data_struct::{BasicInfo, RealTimeInfo};
use crate::dry_run::dry_run;
use crate::get_info::network::network_saver::network_saver;
use crate::utils::{build_urls, connect_ws, init_logger};
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use log::{debug, error, info};
use miniserde::json;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{CpuRefreshKind, DiskRefreshKind, Disks, MemoryRefreshKind, Networks, RefreshKind};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

mod callbacks;
mod command_parser;
mod data_struct;
mod dry_run;
mod get_info;
mod rustls_config;
mod utils;

#[tokio::main]
async fn main() {
    let args = Args::par();

    init_logger(&args.log_level);

    dry_run().await;

    if args.dry_run {
        exit(0);
    }

    let network_config = args.network_config();

    let (http_server, token) = match (args.http_server.clone(), args.token.clone()) {
        (Some(http_server), Some(token)) => (http_server, token),
        (_, _) => {
            error!("The `--http-server` and `--token` parameters must be specified.");
            exit(1);
        }
    };

    for line in args.to_string().lines() {
        debug!("{line}");
    }

    let connection_urls = build_urls(
        http_server.as_ref(),
        args.ws_server.as_ref(),
        token.as_ref(),
    )
    .unwrap_or_else(|e| {
        error!("Failed to parse server address: {e}");
        exit(1);
    });

    for line in connection_urls.to_string().lines() {
        debug!("{line}");
    }

    #[cfg(target_os = "windows")]
    {
        if !args.disable_toast_notify {
            use win_toast_notify::{Action, ActivationType, WinToastNotify};
            WinToastNotify::new()
                .set_title("Komari-monitor-rs Is Running!")
                .set_messages(vec![
                    "Komari-monitor-rs is an application used to monitor your system, granting it near-complete access to your computer. If you did not actively install this program, please check your system immediately. If you have intentionally used this software on your system, please ignore this message or add `--disable-toast-notify` to your startup parameters."
                ])
                .set_actions(vec![
                    Action {
                        activation_type: ActivationType::Protocol,
                        action_content: "komari-monitor".to_string(),
                        arguments: "https://github.com/komari-monitor".to_string(),
                        image_url: None
                    },
                    Action {
                        activation_type: ActivationType::Protocol,
                        action_content: "komari-monitor-rs".to_string(),
                        arguments: "https://github.com/GenshinMinecraft/komari-monitor-rs".to_string(),
                        image_url: None
                    },
                ])
                .show()
                .expect("Failed to show toast notification");
        }
    }

    if !network_config.disable_network_statistics {
        let _listener = tokio::spawn(async move {
            network_saver(&network_config).await;
        });
    } else {
        info!(
            "Network statistics feature disabled. This will fallback to statistics only showing network interface traffic since the current startup"
        );
    }

    loop {
        let Ok(ws_stream) = connect_ws(
            &connection_urls.ws_real_time,
            args.tls,
            args.ignore_unsafe_cert,
        )
        .await
        else {
            error!("Failed to connect to WebSocket server, retrying in 5 seconds");
            sleep(Duration::from_secs(5)).await;
            continue;
        };

        let (write, mut read) = ws_stream.split();

        let locked_write: Arc<
            Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
        > = Arc::new(Mutex::new(write));

        // Handle callbacks
        {
            let args_cloned = args.clone();
            let connection_urls_cloned = connection_urls.clone();
            let locked_write_cloned = locked_write.clone();
            let _listener = tokio::spawn(async move {
                handle_callbacks(
                    &args_cloned,
                    &connection_urls_cloned,
                    &mut read,
                    &locked_write_cloned,
                )
                .await;
            });
        }

        let mut sysinfo_sys = sysinfo::System::new();
        let mut networks = Networks::new_with_refreshed_list();
        let mut disks = Disks::new();
        sysinfo_sys.refresh_cpu_list(
            CpuRefreshKind::nothing()
                .without_cpu_usage()
                .without_frequency(),
        );
        sysinfo_sys.refresh_memory_specifics(MemoryRefreshKind::everything());

        let basic_info = BasicInfo::build(&sysinfo_sys, args.fake, &args.ip_provider).await;

        basic_info.push(connection_urls.basic_info.clone(), args.ignore_unsafe_cert);

        let mut last_network_refresh = tokio::time::Instant::now();
        let mut is_first_network_refresh = true;
        loop {
            let start_time = tokio::time::Instant::now();
            sysinfo_sys.refresh_specifics(
                RefreshKind::nothing()
                    .with_cpu(CpuRefreshKind::everything().without_frequency())
                    .with_memory(MemoryRefreshKind::everything()),
            );
            networks.refresh(true);
            let network_interval = last_network_refresh.elapsed();
            last_network_refresh = tokio::time::Instant::now();
            if is_first_network_refresh {
                // Initialization time makes the first delta unsuitable for a speed sample.
                // Establish a baseline, then wait for a full sampling interval.
                is_first_network_refresh = false;
                sleep(Duration::from_millis(args.realtime_info_interval.max(1))).await;
                continue;
            }
            disks.refresh_specifics(true, DiskRefreshKind::nothing().with_storage());
            let real_time = RealTimeInfo::build(
                &sysinfo_sys,
                &networks,
                &disks,
                args.fake,
                network_interval,
            );

            let json = json::to_string(&real_time);
            {
                let mut write = locked_write.lock().await;
                if let Err(e) = write.send(Message::Text(Utf8Bytes::from(json))).await {
                    error!(
                        "Error occurred while pushing RealTime Info, attempting to reconnect: {e}"
                    );
                    break;
                }
            }
            let end_time = start_time.elapsed();

            sleep(Duration::from_millis({
                let end = u64::try_from(end_time.as_millis()).unwrap_or(0);
                args.realtime_info_interval.saturating_sub(end)
            }))
            .await;
        }
    }
}
