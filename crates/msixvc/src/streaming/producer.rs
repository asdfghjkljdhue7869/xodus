use std::time::Duration;

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
    pub async fn produce(
        &mut self,
        input: &mut ProducerResult,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::File(f) => f.produce(input).await,
            Self::Network(n) => n.produce(input).await,
        }
    }

    fn is_cancelled(&self) -> bool {
        match self {
            Self::File(f) => f.cancellation_token.is_cancelled(),
            Self::Network(n) => n.cancellation_token.is_cancelled(),
        }
    }

    async fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
        match self {
            Self::File(f) => f.cancellation_token.cancelled(),
            Self::Network(n) => n.cancellation_token.cancelled(),
        }
    }
    fn task(&self) -> Result<ProducerTask, flume::TryRecvError> {
        match self {
            Self::File(f) => f.task_pool.try_recv(),
            Self::Network(n) => n.task_pool.try_recv(),
        }
    }
    fn task_retry(&self) -> Result<ProducerTask, flume::TryRecvError> {
        match self {
            Self::File(f) => f.task_retry_pool.try_recv(),
            Self::Network(n) => n.task_retry_pool.try_recv(),
        }
    }
    async fn memory(&self) -> Result<Vec<u8>, flume::RecvError> {
        match self {
            Self::File(f) => f.memory_pool.recv_async().await,
            Self::Network(n) => n.memory_pool.recv_async().await,
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
        log::trace!("Waiting for task");

        let task = loop {
            let _ = tokio::time::sleep(Duration::from_millis(5)).await;
            if producer.is_cancelled() {
                return;
            }
            let task = producer.task_retry().or_else(|_| producer.task());
            match task {
                Err(flume::TryRecvError::Empty) => continue,
                Err(flume::TryRecvError::Disconnected) => {
                    log::error!("Producer channel Closed task, unrecoverable error, exiting task");
                    return;
                }
                Ok(task) => break task,
            }
        };
        log::trace!("Got task");

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

        log::trace!("Producing");
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
