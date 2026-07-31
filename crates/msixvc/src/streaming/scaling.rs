use tokio::sync::mpsc;

use crate::{
    models::streaming::{ProducerTask, StreamProgress},
    streaming::producer,
};

pub struct ProducerScaler {
    producer: Option<producer::StreamProducer>,
    retry_pool: flume::WeakSender<ProducerTask>,
    progress_tx: mpsc::WeakSender<StreamProgress>,
    running_tasks: u16,
    join_set: tokio::task::JoinSet<()>,
}

impl ProducerScaler {
    pub fn new(
        producer: producer::StreamProducer,
        retry_pool: flume::WeakSender<ProducerTask>,
        progress_tx: mpsc::WeakSender<StreamProgress>,
    ) -> Self {
        Self {
            producer: Some(producer),
            running_tasks: 0,
            retry_pool,
            progress_tx,
            join_set: tokio::task::JoinSet::new(),
        }
    }

    pub fn start(&mut self) {
        if let Some(producer) = self.producer.take() {
            let retry_tx = self.retry_pool.clone();
            let progress_tx = self.progress_tx.clone();
            let Some(retry_tx) = retry_tx.upgrade() else {
                log::error!("Unable to upgrade retry sender");
                return;
            };
            let Some(progress_tx) = progress_tx.upgrade() else {
                log::error!("Unable to upgrade progress sender");
                return;
            };
            let new_producer = match producer {
                producer::StreamProducer::File(file_prod) => {
                    producer::StreamProducer::File(file_prod)
                }
                producer::StreamProducer::Network(network_prod) => {
                    let cloned = network_prod.clone();
                    self.producer = Some(producer::StreamProducer::Network(network_prod));
                    producer::StreamProducer::Network(cloned)
                }
            };
            self.join_set.spawn(async move {
                producer::run_producer_loop(new_producer, retry_tx, progress_tx).await;
            });
            self.running_tasks += 1;
            log::debug!("Started new producer task");
        }
    }

    pub fn process_metrics(&mut self) {
        // TODO: Scaling metrics processing
    }

    pub async fn join(self) {
        self.join_set.join_all().await;
    }
}
