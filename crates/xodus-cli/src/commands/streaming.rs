use std::{collections::HashMap, path::Path, process::ExitCode, vec};

use fs2::available_space;
use futures_util::{StreamExt, stream};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use msixvc::{
    models::streaming::{StreamProgress, StreamSource},
    streaming::{self, StreamManager, StreamingParameters},
    xvd::{SegmentFile, XvdFile},
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncRead,
    sync::mpsc::{Receiver, Sender},
};
use uuid::Uuid;
use xodus::tokens::TokenManager;

use crate::{
    license::get_license,
    package::{get_content_id, get_packages},
};

struct Job {
    name: String,
    content: SegmentFile,
}

enum ProgressEvent {
    Started { id: usize, name: String, total: u64 },
    Advanced { id: usize, delta: u64 },
    Finished { id: usize },
    UpdateRemaining { name: String, total: u64 },
    UpdateStatus { name: String },
}

pub async fn run(
    client: &reqwest::Client,
    tokens: &TokenManager,
    source: String,
    destination: String,
    try_skip_ntfs: bool,
    parallel: Option<usize>,
    market: Option<String>,
) -> ExitCode {
    let content_id = if Uuid::try_parse(&source).is_err() {
        let content_id_task = get_content_id(client, source, market.clone()).await;
        let Ok(content_id) = content_id_task else {
            let Err(err) = content_id_task else {
                eprintln!("Unknown Error");
                return ExitCode::FAILURE;
            };
            eprintln!("{}", err);
            return ExitCode::FAILURE;
        };
        content_id
    } else {
        source
    };
    let package_result = get_packages(client, tokens, content_id.clone()).await;
    let Ok(package) = package_result else {
        let Err(err) = package_result else {
            eprintln!("Unknown Error");
            return ExitCode::FAILURE;
        };
        eprintln!("{}", err);
        return ExitCode::FAILURE;
    };
    let Some(file) = package
        .package_files
        .iter()
        .find(|p| p.file_name.ends_with(".msixvc"))
    else {
        eprintln!("No .msixvc file found");
        return ExitCode::FAILURE;
    };
    let urls = file
        .cdn_root_paths
        .iter()
        .map(|p| format!("{}{}", p, file.relative_url))
        .collect();
    let source = StreamSource::Url(urls);

    let (tx, mut rx) = tokio::sync::mpsc::channel(256);

    let (device_key, license) = get_license(
        client,
        tokens,
        content_id,
        market.unwrap_or("NEUTRAL".to_owned()),
    )
    .await
    .expect("Failed to get license");

    let manager = StreamManager::new(StreamingParameters {
        client: client.clone(),
        source,
        content_keys: license
            .content_keys
            .into_iter()
            .map(|(id, key)| {
                let content_key = key.unpack(&device_key).expect("Failed to unpack key");
                (id, *content_key)
            })
            .collect(),
        destination: destination.parse().unwrap(),
        progress_channel: tx,
        max_memory_use: 256 * 1024 * 1024,
    });

    let token = manager.cancellation_token();
    let res = manager.begin();
    let multi_progress = MultiProgress::new();
    let download_progress = multi_progress.add(
                    ProgressBar::new(file.file_size as u64).with_message("Download").with_style(ProgressStyle::with_template("{msg:30!} {bytes:>12}/{total_bytes:>12} {bytes_per_sec:>12} [{bar:40.lime/green}] {percent:>3}%").unwrap().progress_chars("#>-")),
                );
    let write_progress = multi_progress.add(
                    ProgressBar::new(file.file_size as u64).with_message("Disk").with_style(ProgressStyle::with_template("{msg:30!} {bytes:>12}/{total_bytes:>12} {bytes_per_sec:>12} [{bar:40.cyan/blue}] {percent:>3}%").unwrap().progress_chars("#>-")),
                );

    while !res.is_finished() {
        match rx.recv().await {
            Some(StreamProgress::Download(downloaded)) => {
                download_progress.inc(downloaded);
            }

            Some(StreamProgress::Write(written)) => {
                write_progress.inc(written);
            }

            Some(StreamProgress::Resume(resume)) => {
                download_progress.set_position(resume);
                write_progress.set_position(resume);
            }
            None => break,
        }
    }
    download_progress.finish_and_clear();
    write_progress.finish_and_clear();

    println!("{:?}", res.await);
    ExitCode::SUCCESS
}
