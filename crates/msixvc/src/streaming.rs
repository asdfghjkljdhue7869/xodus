use crate::{
    math::{offset_to_page_number, page_number_to_offset},
    models::{
        streaming::{DecryptionResult, ProducerResult, ProducerTask, StreamProgress, StreamSource},
        xvd::{XvcInfo, XvcRegionFlags, XvcRegionHeader, XvdHeader},
    },
    streaming::producer::StreamProducer,
    xvd::{SegmentFile, XvdFile},
};
use std::{collections::HashMap, os::unix::fs::FileExt};
use std::{os::unix::fs::MetadataExt, path::PathBuf};
use tokio::io::BufReader;
use tokio_util::sync::CancellationToken;

pub mod decryption;
pub mod producer;
pub mod scaling;
pub mod writer;

pub const STREAM_PAGE_SIZE: usize = 4096;
pub const STREAM_PAGES_PER_BUFFER: usize = 128;
pub const STREAM_RESULT_BUFFER_SIZE: usize = STREAM_PAGES_PER_BUFFER * STREAM_PAGE_SIZE;

pub struct StreamingParameters {
    pub client: reqwest::Client,
    pub source: StreamSource,
    pub content_keys: Vec<(uuid::Uuid, [u8; 32])>,
    pub destination: PathBuf,
    pub progress_channel: tokio::sync::mpsc::Sender<StreamProgress>,
    /// Bytes
    pub max_memory_use: u32,
}

type FlumePair<T> = (flume::Sender<T>, flume::Receiver<T>);

struct WorkerChannels {
    task_pool: FlumePair<ProducerTask>,
    task_retry_pool: FlumePair<ProducerTask>,
    memory_pool: FlumePair<Vec<u8>>,
    producer_result_pool: FlumePair<ProducerResult>,
    decryption_result_pool: FlumePair<DecryptionResult>,
}

impl WorkerChannels {
    pub fn new(cap: usize) -> Self {
        Self {
            task_pool: flume::unbounded(),
            task_retry_pool: flume::unbounded(),
            memory_pool: flume::bounded(cap),
            producer_result_pool: flume::unbounded(),
            decryption_result_pool: flume::unbounded(),
        }
    }
}

pub struct StreamManager {
    parameters: StreamingParameters,
    worker_channels: WorkerChannels,
    cancellation_token: CancellationToken,
}

impl StreamManager {
    pub fn new(parameters: StreamingParameters) -> Self {
        let memory_chunks_cap = parameters.max_memory_use as usize / STREAM_RESULT_BUFFER_SIZE;
        Self {
            parameters,
            worker_channels: WorkerChannels::new(memory_chunks_cap),
            cancellation_token: CancellationToken::new(),
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub fn begin(
        self,
    ) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        self.allocate_memory();
        tokio::spawn(async move {
            let mut rfiles: HashMap<String, SegmentFile> = HashMap::new();
            let mut lfiles: HashMap<String, SegmentFile> = HashMap::new();
            let producer = self.initialize_producer().await?;
            let mut scale = scaling::ProducerScaler::new(
                producer,
                self.worker_channels.task_retry_pool.0.clone().downgrade(),
            );
            scale.start();

            tokio::fs::create_dir_all(&self.parameters.destination).await?;
            let resident_file_name = self.run_bootstrap().await?;

            let mut resident_file =
                tokio::fs::File::open(self.parameters.destination.join(resident_file_name)).await?;
            let xvd_file = XvdFile::parse(&mut resident_file).await?;

            let files = xvd_file
                .parse_user_package_files(&mut resident_file)
                .await?;
            for (k, v) in &files {
                if k == "SegmentMetadata.bin" {
                    let sfiles = xvd_file
                        .parse_segment_metadata(&mut resident_file, v)
                        .await?;
                    rfiles = sfiles;
                    break;
                }
            }
            let sfiles = xvd_file
                .parse_ntfs_segment_metadata(&mut resident_file, !rfiles.is_empty())
                .await?;
            rfiles.extend(sfiles);

            self.cancellation_token.cancel();
            scale.join().await;
            Ok(())
        })
    }

    async fn run_bootstrap(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        log::trace!("Started bootstrap");
        let task_tx = &self.worker_channels.task_pool.0;
        let memory_tx = &self.worker_channels.memory_pool.0;
        let rx = &self.worker_channels.producer_result_pool.1;

        task_tx
            .send_async(ProducerTask::Download {
                page_number: 0,
                number_of_pages: 1,
            })
            .await?;
        let xvd_frame = rx.recv_async().await?;
        let xvd_header = XvdHeader::from_slice(&xvd_frame.buffer)?;
        memory_tx.send_async(xvd_frame.buffer).await?;
        let uuid = xvd_header.vduid.to_string().to_uppercase();

        let (_hash_tree_levels, hash_tree_page_count) = xvd_header.hash_tree_info();
        let xvc_info_offset = xvd_header.xvc_info_offset(hash_tree_page_count);
        let mut region_headers = Vec::with_capacity(0);
        if xvc_info_offset > 0 {
            log::trace!("Requesting xvc info");
            task_tx
                .send_async(ProducerTask::Download {
                    page_number: offset_to_page_number(xvc_info_offset),
                    number_of_pages: 2,
                })
                .await?;
            let xvc_frame = rx.recv_async().await?;
            let mut xvc_cursor = std::io::Cursor::new(&xvc_frame.buffer);
            let xvc_info = XvcInfo::read(&mut xvc_cursor).await?;
            region_headers.reserve(xvc_info.region_count as usize);
            for _ in 0..xvc_info.region_count {
                let region_header = XvcRegionHeader::read(&mut xvc_cursor).await?;
                region_headers.push(region_header);
            }
            memory_tx.send_async(xvc_frame.buffer).await?;
        }

        let resident_headers: Vec<&XvcRegionHeader> = region_headers
            .iter()
            .filter(|pred| pred.flags.contains(XvcRegionFlags::RESIDENT))
            .collect();

        let resident_length = resident_headers.into_iter().try_fold(0, |o, h| {
            if o != h.offset {
                return Err("Unexpected gap in resident region headers".into());
            }
            let new_extent = h.offset + h.length;
            Ok::<u64, String>(new_extent)
        })?;

        assert!(resident_length.is_multiple_of(STREAM_PAGE_SIZE as u64));
        let resident_path = self.parameters.destination.join(&uuid);

        // TODO: add a version check too, and possibly a continuation codepath
        if tokio::fs::metadata(&resident_path)
            .await
            .is_ok_and(|m| m.size() == resident_length)
        {
            return Ok(uuid);
        }

        // Schedule full resident stream
        let buffer_count = resident_length.div_ceil(STREAM_RESULT_BUFFER_SIZE as u64);
        let mut pages_left = resident_length / STREAM_PAGE_SIZE as u64;
        for i in 0..buffer_count {
            let number_of_pages = pages_left.min(STREAM_PAGES_PER_BUFFER as u64);
            let message = ProducerTask::Download {
                page_number: i * STREAM_PAGES_PER_BUFFER as u64,
                number_of_pages,
            };
            pages_left -= number_of_pages;
            task_tx.send_async(message).await?;
        }

        let rx = rx.clone();
        let memory_tx = memory_tx.clone();
        tokio::task::spawn_blocking(move || {
            let resident_file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(resident_path)?;

            let mut downloaded = 0;
            while downloaded < buffer_count {
                let result = rx.recv()?;
                let start_offset = page_number_to_offset(result.page_number);
                let size = page_number_to_offset(result.number_of_pages);
                resident_file.write_all_at(&result.buffer[..size as usize], start_offset)?;
                downloaded += 1;
                memory_tx.send(result.buffer)?;
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await??;
        Ok(uuid)
    }

    async fn run_transfer(&self, producer: StreamProducer, layout: ()) {
        todo!("Full transfer pipeline");
    }

    async fn initialize_producer(
        &self,
    ) -> Result<StreamProducer, Box<dyn std::error::Error + Send + Sync>> {
        let producer = match &self.parameters.source {
            StreamSource::File(file_path) => self.initialize_file_producer(file_path).await?,
            StreamSource::Url(url) => self.initialize_network_producer(url).await?,
        };
        Ok(producer)
    }

    async fn initialize_file_producer(&self, file_path: &str) -> tokio::io::Result<StreamProducer> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(file_path)
            .await?;
        let task_pool = self.worker_channels.task_pool.1.clone();
        let task_retry_pool = self.worker_channels.task_retry_pool.1.clone();
        let memory_pool = self.worker_channels.memory_pool.1.clone();
        let result_pool = self.worker_channels.producer_result_pool.0.clone();
        Ok(StreamProducer::File(producer::file::FileProducer::new(
            file,
            self.cancellation_token(),
            task_pool,
            task_retry_pool,
            memory_pool,
            result_pool,
        )))
    }

    async fn initialize_network_producer(&self, url: &str) -> tokio::io::Result<StreamProducer> {
        let client = self.parameters.client.clone();
        let task_pool = self.worker_channels.task_pool.1.clone();
        let task_retry_pool = self.worker_channels.task_retry_pool.1.clone();
        let memory_pool = self.worker_channels.memory_pool.1.clone();
        let result_pool = self.worker_channels.producer_result_pool.0.clone();
        Ok(StreamProducer::Network(
            producer::network::NetworkProducer::new(
                client,
                url.to_owned(),
                self.cancellation_token(),
                task_pool,
                task_retry_pool,
                memory_pool,
                result_pool,
            ),
        ))
    }

    fn allocate_memory(&self) {
        let memory_chunks_cap = self.parameters.max_memory_use as usize / STREAM_RESULT_BUFFER_SIZE;
        for _ in 0..memory_chunks_cap {
            let buffer: Vec<u8> = vec![0; STREAM_RESULT_BUFFER_SIZE];
            let _ = self.worker_channels.memory_pool.0.send(buffer);
        }
    }
}
