use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::StreamProducer;
use crate::models::streaming::{ProducerResult, ProducerTask};

/// Pipeline producer from File source
pub struct FileProducer {
    file: tokio::fs::File,
    page_size: usize,

    cancellation_token: tokio_util::sync::CancellationToken,
    task_pool: flume::Receiver<ProducerTask>,
    task_retry_pool: flume::Receiver<ProducerTask>,
    memory_pool: flume::Receiver<Vec<u8>>,
    result_pool: flume::Sender<ProducerResult>,
}

impl FileProducer {
    pub fn new(
        file: tokio::fs::File,
        page_size: usize,
        cancellation_token: tokio_util::sync::CancellationToken,
        task_pool: flume::Receiver<ProducerTask>,
        task_retry_pool: flume::Receiver<ProducerTask>,
        memory_pool: flume::Receiver<Vec<u8>>,
        result_pool: flume::Sender<ProducerResult>,
    ) -> Self {
        Self {
            file,
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
impl StreamProducer for FileProducer {
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
        let start_offset = input.page_number * self.page_size as u64;
        let size = input.number_of_pages * self.page_size as u64;
        let mut read_total = 0;
        assert!(
            buffer.len() >= size as usize,
            "Buffer size is smaller than requested"
        );

        self.file
            .seek(std::io::SeekFrom::Start(start_offset))
            .await?;

        while read_total < size as usize {
            let read = self.file.read(&mut buffer[read_total..]).await?;
            let difference = read_total.abs_diff(size as usize);
            if read == 0 && difference > 0 {
                buffer[read_total..read_total + difference].fill(0);
                break;
            }
            read_total += read;
        }

        Ok(())
    }
}
