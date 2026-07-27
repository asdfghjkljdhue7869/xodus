use futures_util::StreamExt;

use super::StreamProducer;
use crate::models::streaming::{ProducerResult, ProducerTask};

/// Pipeline producer from network source
#[derive(Clone)]
pub struct NetworkProducer {
    url: String,
    client: reqwest::Client,
    page_size: usize,

    cancellation_token: tokio_util::sync::CancellationToken,

    task_pool: flume::Receiver<ProducerTask>,
    task_retry_pool: flume::Receiver<ProducerTask>,
    memory_pool: flume::Receiver<Vec<u8>>,
    result_pool: flume::Sender<ProducerResult>,
}

impl NetworkProducer {
    pub fn new(
        client: reqwest::Client,
        url: String,
        page_size: usize,

        cancellation_token: tokio_util::sync::CancellationToken,
        task_pool: flume::Receiver<ProducerTask>,
        task_retry_pool: flume::Receiver<ProducerTask>,
        memory_pool: flume::Receiver<Vec<u8>>,
        result_pool: flume::Sender<ProducerResult>,
    ) -> Self {
        Self {
            url,
            client,
            page_size,
            cancellation_token,
            task_pool,
            task_retry_pool,
            memory_pool,
            result_pool,
        }
    }
}

#[async_trait::async_trait]
impl StreamProducer for NetworkProducer {
    async fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
        self.cancellation_token.cancelled()
    }
    async fn task(&self) -> Result<ProducerTask, flume::RecvError> {
        self.task_pool.recv_async().await
    }
    async fn task_retry(&self) -> Result<ProducerTask, flume::RecvError> {
        self.task_retry_pool.recv_async().await
    }
    async fn memory(&self) -> Result<Vec<u8>, flume::RecvError> {
        self.memory_pool.recv_async().await
    }
    fn send_result(&self, result: ProducerResult) -> Result<(), flume::SendError<ProducerResult>> {
        self.result_pool.send(result)
    }
    async fn produce(
        &mut self,
        input: &mut ProducerResult,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let buffer = &mut input.buffer;
        let range_start = input.page_number * self.page_size as u64;
        let size = input.number_of_pages * self.page_size as u64;
        let range_end = range_start + size;

        let range_header = format!("bytes={range_start}-{range_end}");

        let response = self
            .client
            .get(&self.url)
            .header("Range", range_header)
            .send()
            .await?;

        let mut stream = response.bytes_stream();
        let mut read_total = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let read = chunk.len();
            buffer[read_total..].copy_from_slice(&chunk);
            read_total += read;
        }
        let difference = read_total.abs_diff(size as usize);
        buffer[read_total..read_total + difference].fill(0);

        Ok(())
    }
}
