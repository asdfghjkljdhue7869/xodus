use crate::models::streaming::{ProducerResult, ProducerTask};

pub mod file;
pub mod network;

#[async_trait::async_trait]
pub trait StreamProducer {
    async fn produce(
        &self,
        input: &mut ProducerResult,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_>;
    async fn task(&self) -> Result<ProducerTask, flume::RecvError>;
    async fn task_retry(&self) -> Result<ProducerTask, flume::RecvError>;
    async fn memory(&self) -> Result<Vec<u8>, flume::RecvError>;
    fn send_result(&self, result: ProducerResult) -> Result<(), flume::SendError<ProducerResult>>;
}

pub async fn run_producer_loop<T>(producer: T, retry_tx: flume::Sender<ProducerResult>)
where
    T: StreamProducer,
{
    loop {
        let task = tokio::select! {
            retry_task = producer.task_retry() => retry_task,
            task = producer.task() => task,
            _ = producer.cancelled() => break,
        };

        let Ok(task) = task else {
            log::error!("Producer got RecvError on task, unrecoverable error, exiting task");
            break;
        };

        let mut result = match task {
            ProducerTask::Stop => break,
            ProducerTask::Retry(mut retry) => {
                retry.retry_number += 1;
                retry
            }
            ProducerTask::Download {
                page_number,
                number_of_pages,
            } => {
                let Ok(memory) = producer.memory().await else {
                    log::error!(
                        "Producer got RecvError on memory(), unrecoverable error, exiting task"
                    );
                    break;
                };
                ProducerResult {
                    page_number: page_number,
                    number_of_pages: number_of_pages,
                    retry_number: 0,
                    buffer: memory,
                }
            }
        };

        let produce_result = producer.produce(&mut result).await;

        match produce_result {
            Ok(()) => {
                if let Err(err) = producer.send_result(result) {
                    log::error!("Producer got SendError, unercoverable error, exiting task");
                    let _ = retry_tx.send(err.0);
                    break;
                }
            }
            Err(err) => {
                log::error!("Failed to produce requested resource {err:?}");
                if result.retry_number < 10 {
                    let _ = retry_tx.send(result);
                }
            }
        }
    }
}
