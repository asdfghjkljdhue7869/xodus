use crate::{
    models::streaming::{
        DecryptionResult, ProducerResult, ProducerTask, StreamProgress, StreamSource,
    },
    streaming::producer::StreamProducer,
};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

pub mod decryption;
pub mod producer;
pub mod scaling;
pub mod writer;

pub const STREAM_PAGE_SIZE: usize = 4096;
pub const STREAM_RESULT_BUFFER_SIZE: usize = 64 * STREAM_PAGE_SIZE;

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
        mut self,
    ) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        self.allocate_memory();

        tokio::spawn(async move {
            let producer = self.initialize_producer().await?;
            let mut scaler = scaling::ProducerScaler::new(
                producer,
                self.worker_channels.task_retry_pool.0.clone().downgrade(),
            );
            scaler.start();

            // Phase 1 - headers parsing
            // Request XVC header - 1st page
            // Parse XVC header - calculate data offsets
            // Request new data, setup headers cache file `{CONTENT_ID}`

            loop {
                if self.cancellation_token.is_cancelled() {
                    break;
                }
                scaler.process_metrics();
            }

            scaler.join().await;
            Ok(())
        })
    }

    async fn initialize_producer(
        &mut self,
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
