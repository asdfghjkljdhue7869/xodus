use crate::models::streaming::{ProducerResult, ProducerTask};

pub mod file;
pub mod network;

// pub trait StreamProducer {
//     async fn produce(
//         &mut self,
//         input: &mut ProducerResult,
//     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
//     async fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_>;
//     async fn task(&self) -> Result<ProducerTask, flume::RecvError>;
//     async fn task_retry(&self) -> Result<ProducerTask, flume::RecvError>;
//     async fn memory(&self) -> Result<Vec<u8>, flume::RecvError>;
//     fn send_result(&self, result: ProducerResult) -> Result<(), flume::SendError<ProducerResult>>;
//     fn supports_scalability(&self) -> bool;
// }

pub enum StreamProducer {
    Network(network::NetworkProducer),
    File(file::FileProducer),
}

impl StreamProducer {
    async fn produce(
        &mut self,
        input: &mut ProducerResult,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::File(f) => f.produce(input).await,
            Self::Network(n) => n.produce(input).await,
        }
    }

    async fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
        match self {
            Self::File(f) => f.cancellation_token.cancelled(),
            Self::Network(n) => n.cancellation_token.cancelled(),
        }
    }
    async fn task(&self) -> Result<ProducerTask, flume::RecvError> {
        match self {
            Self::File(f) => f.task_pool.recv_async().await,
            Self::Network(n) => n.task_pool.recv_async().await,
        }
    }
    async fn task_retry(&self) -> Result<ProducerTask, flume::RecvError> {
        match self {
            Self::File(f) => f.task_retry_pool.recv_async().await,
            Self::Network(n) => n.task_retry_pool.recv_async().await,
        }
    }
    async fn memory(&self) -> Result<Vec<u8>, flume::RecvError> {
        match self {
            Self::File(f) => f.memory_pool.recv(),
            Self::Network(n) => n.memory_pool.recv(),
        }
    }
    fn send_result(&self, result: ProducerResult) -> Result<(), flume::SendError<ProducerResult>> {
        match self {
            Self::File(f) => f.result_pool.send(result),
            Self::Network(n) => n.result_pool.send(result),
        }
    }
}

pub async fn run_producer_loop(
    mut producer: StreamProducer,
    retry_tx: flume::Sender<ProducerTask>,
) {
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
            ProducerTask::End => break,
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
                    page_number,
                    number_of_pages,
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
                    let _ = retry_tx.send(ProducerTask::Retry(err.0));
                    break;
                }
            }
            Err(err) => {
                log::error!("Failed to produce requested resource {err:?}");
                if result.retry_number < 10 {
                    let _ = retry_tx.send(ProducerTask::Retry(result));
                } else {
                    todo!("Handle retries issue");
                }
            }
        }
    }
}
