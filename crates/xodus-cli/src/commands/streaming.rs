use std::{collections::HashMap, path::Path, process::ExitCode, vec};

use fs2::available_space;
use futures_util::{StreamExt, stream};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use msixvc::{
    models::streaming::StreamSource,
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
    let source = if source.starts_with("file://") {
        let fsrc = source.strip_prefix("file://").unwrap_or_default();
        StreamSource::File(fsrc.to_owned())
    } else {
        let vurl = if source.starts_with("http://") || source.starts_with("https://") {
            source
        } else {
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
            format!(
                "{}{}",
                file.cdn_root_paths.first().unwrap(),
                file.relative_url
            )
        };
        StreamSource::Url(vurl)
    };

    let (tx, rx) = tokio::sync::mpsc::channel(256);

    let manager = StreamManager::new(StreamingParameters {
        client: client.clone(),
        source,
        content_keys: vec![],
        destination: destination.parse().unwrap(),
        progress_channel: tx,
        max_memory_use: 256 * 1024 * 1024,
    });

    let token = manager.cancellation_token();
    let res = manager.begin().await;

    println!("{res:?}");

    ExitCode::SUCCESS
}
