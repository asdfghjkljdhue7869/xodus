use futures_util::StreamExt;

use crate::models::streaming::{ProducerResult, ProducerTask};

/// Pipeline producer from network source
#[derive(Clone)]
pub struct NetworkProducer {
    url: String,
    client: reqwest::Client,

    pub(super) cancellation_token: tokio_util::sync::CancellationToken,

    pub(super) task_pool: flume::Receiver<ProducerTask>,
    pub(super) task_retry_pool: flume::Receiver<ProducerTask>,
    pub(super) memory_pool: flume::Receiver<Vec<u8>>,
    pub(super) result_pool: flume::Sender<ProducerResult>,
}

impl NetworkProducer {
    pub fn new(
        client: reqwest::Client,
        url: String,

        cancellation_token: tokio_util::sync::CancellationToken,
        task_pool: flume::Receiver<ProducerTask>,
        task_retry_pool: flume::Receiver<ProducerTask>,
        memory_pool: flume::Receiver<Vec<u8>>,
        result_pool: flume::Sender<ProducerResult>,
    ) -> Self {
        Self {
            url,
            client,
            cancellation_token,
            task_pool,
            task_retry_pool,
            memory_pool,
            result_pool,
        }
    }

    pub async fn produce(
        &mut self,
        input: &mut ProducerResult,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let buffer = &mut input.buffer;
        let range_start = input.page_number * 4096 as u64;
        let size = input.number_of_pages * 4096 as u64;
        let range_end = range_start + size - 1;

        let range_header = format!("bytes={range_start}-{range_end}");
        log::trace!("Requesting {range_header}");
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
            buffer[read_total..read_total + chunk.len()].copy_from_slice(&chunk);
            read_total += read;
        }
        let difference = read_total.abs_diff(size as usize);
        buffer[read_total..read_total + difference].fill(0);

        Ok(())
    }
}
