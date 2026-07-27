use super::StreamProducer;
use crate::models::streaming::{ProducerResult, ProducerTask};

/// Pipeline producer from File source
pub struct FileProducer {
    file: tokio::fs::File,

    cancellation_token: tokio_util::sync::CancellationToken,
    task_pool: flume::Receiver<ProducerTask>,
    task_retry_pool: flume::Receiver<ProducerTask>,
    memory_pool: flume::Receiver<Vec<u8>>,
    result_pool: flume::Sender<ProducerResult>,
}

impl FileProducer {
    pub fn new(
        file: tokio::fs::File,
        cancellation_token: tokio_util::sync::CancellationToken,
        task_pool: flume::Receiver<ProducerTask>,
        task_retry_pool: flume::Receiver<ProducerTask>,
        memory_pool: flume::Receiver<Vec<u8>>,
        result_pool: flume::Sender<ProducerResult>,
    ) -> Self {
        Self {
            file,
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
        &self,
        input: &mut ProducerResult,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let buffer = &mut input.buffer;
        todo!("File producer is unimplemented");
        Ok(())
    }
}
