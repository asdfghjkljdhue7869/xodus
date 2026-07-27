pub enum ProducerTask {
    Download {
        page_number: u64,
        number_of_pages: u64,
    },
    Retry(ProducerResult),
}

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
