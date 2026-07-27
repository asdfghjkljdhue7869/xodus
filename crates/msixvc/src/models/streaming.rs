pub enum ProducerTask {
    Download {
        page_number: usize,
        number_of_pages: usize,
    },
    Retry(ProducerResult),
    Stop,
}

pub struct ProducerResult {
    pub page_number: usize,
    pub number_of_pages: usize,
    pub retry_number: usize,
    pub buffer: Vec<u8>,
}

pub struct DecryptionResult {
    pub page_number: usize,
    pub number_of_pages: usize,
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
