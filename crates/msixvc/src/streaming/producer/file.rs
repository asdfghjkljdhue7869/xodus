use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::models::streaming::{ProducerResult, ProducerTask};

/// Pipeline producer from File source
#[derive(Debug)]
pub struct FileProducer {
    file: tokio::fs::File,

    pub(super) cancellation_token: tokio_util::sync::CancellationToken,

    pub(super) task_pool: flume::Receiver<ProducerTask>,
    pub(super) task_retry_pool: flume::Receiver<ProducerTask>,
    pub(super) memory_pool: flume::Receiver<Vec<u8>>,
    pub(super) result_pool: flume::Sender<ProducerResult>,
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

    pub async fn produce(
        &mut self,
        input: &mut ProducerResult,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let buffer = &mut input.buffer;
        let start_offset = input.page_number * 4096 as u64;
        let size = input.number_of_pages * 4096 as u64;
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
