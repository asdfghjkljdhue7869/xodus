#[derive(Debug)]
pub enum ProducerTask {
    Download {
        page_number: u64,
        number_of_pages: u64,
    },
    Retry(ProducerResult),
    End,
}

#[derive(Debug)]
pub struct ProducerResult {
    pub page_number: u64,
    pub number_of_pages: u64,
    pub retry_number: u8,
    pub buffer: Vec<u8>,
}

pub struct DecryptionResult {
    pub page_number: u64,
    pub number_of_pages: u64,
    pub buffer: Vec<u8>,
}

impl From<ProducerResult> for DecryptionResult {
    fn from(value: ProducerResult) -> Self {
        Self {
            page_number: value.page_number,
            number_of_pages: value.number_of_pages,
            buffer: value.buffer,
        }
    }
}
pub enum StreamSource {
    File(String),
    Url(String),
}

impl From<String> for StreamSource {
    fn from(value: String) -> Self {
        if value.starts_with("http") {
            Self::Url(value)
        } else {
            Self::File(value)
        }
    }
}

pub enum StreamProgress {
    Download(StreamProgressUpdate),
    Write(StreamProgressUpdate),
}

pub struct StreamProgressUpdate {
    pub processed: u64,
    pub total: u64,
}
