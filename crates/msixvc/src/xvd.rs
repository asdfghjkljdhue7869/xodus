use aes::Aes128;
use aes::cipher::KeyInit;
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::header::RANGE;
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::task::block_in_place;
use tokio::time::{sleep, timeout};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tokio_util::io::SyncIoBridge;
use zerocopy::IntoBytes;

use crate::models::xvd::{
    PAGE_SIZE, PAGES_PER_BLOCK, XvdSegmentMetadataHeader, XvdSegmentMetadataSegment,
    XvdSegmentMetadataSegmentFlags, XvdUserDataHeader, XvdUserDataPackageFileEntry,
    XvdUserDataPackageFilesHeader,
};
use crate::streaming_ntfs::collect_ntfs_stream_layouts;

use crate::crypt::{Tweak, decrypt_page_xts};
use crate::math::{
    bytes_to_pages, calculate_hash_block_num_and_run_for_block_num, offset_to_page_number,
};
use crate::{
    math::page_number_to_offset,
    models::xvd::{XvcInfo, XvcRegionHeader, XvcRegionId, XvdHashEntry, XvdHeader},
};

pub struct SyncSubstream<R> {
    inner: R,
    start: u64,
    len: u64,
    pos: u64,
}

impl<R> Debug for SyncSubstream<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncSubstream")
            .field("start", &self.start)
            .field("len", &self.len)
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

impl<R> SyncSubstream<R> {
    pub fn new(inner: R, start: u64, len: u64) -> Self {
        Self {
            inner,
            start,
            len,
            pos: 0,
        }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn into_inner(self) -> R {
        self.inner
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }
}

impl<R: Read + Seek> Read for SyncSubstream<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.len {
            return Ok(0);
        }

        let remaining = usize::try_from(self.len - self.pos)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "remaining range too large"))?;
        let to_read = remaining.min(buf.len());

        self.inner.seek(SeekFrom::Start(self.start + self.pos))?;
        let read = self.inner.read(&mut buf[..to_read])?;
        self.pos += read as u64;
        Ok(read)
    }
}

impl<R: Seek> Seek for SyncSubstream<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let next = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(delta) => {
                if delta >= 0 {
                    self.pos.checked_add(delta as u64).ok_or_else(|| {
                        Error::new(ErrorKind::InvalidInput, "invalid relative seek")
                    })?
                } else {
                    self.pos.checked_sub(delta.unsigned_abs()).ok_or_else(|| {
                        Error::new(ErrorKind::InvalidInput, "invalid relative seek")
                    })?
                }
            }
            SeekFrom::End(delta) => {
                if delta >= 0 {
                    self.len.checked_add(delta as u64).ok_or_else(|| {
                        Error::new(ErrorKind::InvalidInput, "invalid end-relative seek")
                    })?
                } else {
                    self.len.checked_sub(delta.unsigned_abs()).ok_or_else(|| {
                        Error::new(ErrorKind::InvalidInput, "invalid end-relative seek")
                    })?
                }
            }
        };

        if next > self.len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "seek past substream end",
            ));
        }

        self.pos = next;
        Ok(self.pos)
    }
}

impl<R: Write + Seek> Write for SyncSubstream<R> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.pos >= self.len {
            return Ok(0);
        }

        let remaining = usize::try_from(self.len - self.pos)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "remaining range too large"))?;
        let to_write = remaining.min(buf.len());

        self.inner.seek(SeekFrom::Start(self.start + self.pos))?;
        let written = self.inner.write(&buf[..to_write])?;
        self.pos += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct XvdStream<R> {
    inner: R,
    offset: u64,
    end_offset: u64,
}

impl<R> Debug for XvdStream<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XvdStream")
            .field("offset", &self.offset)
            .field("end_offset", &self.end_offset)
            .finish_non_exhaustive()
    }
}

impl<R> XvdStream<R> {
    fn len(&self) -> u64 {
        self.end_offset - self.offset
    }

    fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Seek> XvdStream<R> {
    fn current_relative_pos(&mut self) -> std::io::Result<u64> {
        let absolute = self.inner.stream_position()?;
        absolute
            .checked_sub(self.offset)
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, "stream before virtual start"))
    }
}

impl<R: Read + Seek> Read for XvdStream<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let current = self.current_relative_pos()?;
        if current >= self.len() {
            return Ok(0);
        }

        let remaining = usize::try_from(self.len() - current)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "remaining range too large"))?;
        let to_read = remaining.min(buf.len());

        self.inner.read(&mut buf[..to_read])
    }
}

impl<R: Seek> Seek for XvdStream<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_relative = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::Current(delta) => {
                let current = self.current_relative_pos()?;
                if delta >= 0 {
                    current.checked_add(delta as u64)
                } else {
                    current.checked_sub(delta.unsigned_abs())
                }
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "invalid relative seek"))?
            }
            SeekFrom::End(delta) => {
                let len = self.len();
                if delta >= 0 {
                    len.checked_add(delta as u64)
                } else {
                    len.checked_sub(delta.unsigned_abs())
                }
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "invalid end-relative seek"))?
            }
        };

        if new_relative > self.len() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "seek past virtual device end",
            ));
        }

        self.inner
            .seek(SeekFrom::Start(self.offset + new_relative))?;
        Ok(new_relative)
    }
}

impl<R> Write for XvdStream<R> {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(Error::new(
            ErrorKind::PermissionDenied,
            "XvdStream is read-only",
        ))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub struct XvdFile {
    header: XvdHeader,
    drive_data_offset: u64,
    encryption_key_ids: Vec<uuid::Uuid>,
    encrypted_regions: Vec<XvcRegionHeader>,
    user_data_offset: u64,
}

pub struct UserPackageFile {
    pub offset: u64,
    pub length: u64,
}

pub struct SegmentFile {
    pub offset: u64,
    pub length: u64,
    pub data_hashs: Vec<[u8; 20]>,
    pub keep_encrypted: bool,
}

impl XvdFile {
    pub fn content_id(&self) -> uuid::Uuid {
        self.header.vduid
    }

    fn non_encrypted_prefix_len(&self, start: u64, len: u64) -> u64 {
        let end = start.saturating_add(len);
        let mut prefix_len = len;

        for section in &self.encrypted_regions {
            let section_start = section.offset;
            let section_end = section.offset.saturating_add(section.length);

            if section_end <= start || section_start >= end {
                continue;
            }

            if start >= section_start {
                return 0;
            }

            prefix_len = section_start.saturating_sub(start);
            break;
        }

        prefix_len
    }

    pub async fn parse_file(
        path: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut file = OpenOptions::new().read(true).open(path.clone()).await?;
        Self::parse(&mut file).await
    }

    pub async fn parse<Reader>(
        mut file: Reader,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        Reader: AsyncRead + AsyncSeek + Unpin,
    {
        log::trace!("Parsing XvdFile");
        file.rewind().await?;
        let xvd_header = XvdHeader::read(&mut file).await?;

        let mdu_offset = xvd_header.mdu_offset();
        let (_hash_tree_levels, hash_tree_page_count) = xvd_header.hash_tree_info();
        let xvc_info_offset = xvd_header.xvc_info_offset(hash_tree_page_count);

        let mut region_headers: Vec<XvcRegionHeader> = Vec::new();
        let mut encryption_key_ids = Vec::with_capacity(0);
        // TODO: Check if we have proper content type
        if xvd_header.xvc_data_length > 0 {
            file.seek(std::io::SeekFrom::Start(xvc_info_offset))
                .await
                .expect("Unable to seek");
            let xvc_info = XvcInfo::read(&mut file).await?;
            encryption_key_ids = xvc_info.xvc_encryption_key_ids;
            let region_count = xvc_info.region_count;

            if xvc_info.version >= 1 {
                for _ in 0..region_count {
                    let region_header = XvcRegionHeader::read(&mut file).await?;
                    region_headers.push(region_header);
                }
            }
        }

        let hash_tree_offset = xvd_header.mutable_data_length() + mdu_offset;
        let user_data_offset = if xvd_header.volume_flags.is_data_integrity_enabled() {
            page_number_to_offset(xvd_header.hash_tree_info().1)
        } else {
            0
        } + hash_tree_offset;
        let xvc_info_offset =
            page_number_to_offset(xvd_header.user_data_page_count()) + user_data_offset;
        let dynamic_header_offset =
            page_number_to_offset(xvd_header.xvc_data_page_count()) + xvc_info_offset;
        let drive_data_offset =
            page_number_to_offset(xvd_header.dynamic_header_page_count()) + dynamic_header_offset;

        let encrypted_regions = region_headers
            .iter()
            .filter(|reg| reg.key_id.is_encrypted())
            .cloned()
            .collect();

        log::trace!("Parsing XvdFile - complete");
        Ok(XvdFile {
            header: xvd_header,
            encryption_key_ids,
            drive_data_offset,
            encrypted_regions,
            user_data_offset,
        })
    }

    pub async fn parse_user_package_files<Reader>(
        &self,
        mut file: Reader,
    ) -> Result<HashMap<String, UserPackageFile>, Box<dyn std::error::Error + Send + Sync>>
    where
        Reader: AsyncRead + AsyncSeek + Unpin,
    {
        log::trace!("Parsing user package files");
        let mut files = HashMap::new();

        let user_data_offset = self.user_data_offset;
        file.seek(SeekFrom::Start(user_data_offset)).await?;
        let user_data_header = XvdUserDataHeader::read(&mut file).await?;
        if user_data_header.t == 0 {
            let mut off = user_data_offset + user_data_header.length as u64;
            file.seek(SeekFrom::Start(off)).await?;
            let user_data_package_files_header =
                XvdUserDataPackageFilesHeader::read(&mut file).await?;
            off += XvdUserDataPackageFilesHeader::RAW_SIZE as u64;
            for _ in 0..user_data_package_files_header.file_count {
                file.seek(SeekFrom::Start(off)).await?;
                let user_data_package_file_entry =
                    XvdUserDataPackageFileEntry::read(&mut file).await?;
                off += XvdUserDataPackageFileEntry::RAW_SIZE as u64;
                let o = user_data_package_file_entry.offset;
                let s: u32 = user_data_package_file_entry.size;
                let fullname = user_data_package_file_entry.file_path;
                let end = fullname
                    .iter()
                    .position(|&c| c == 0)
                    .unwrap_or(fullname.len());
                let pfull_name: String = String::from_utf16(&fullname[..end]).unwrap();

                files.insert(
                    pfull_name,
                    UserPackageFile {
                        offset: user_data_offset + XvdUserDataHeader::RAW_SIZE as u64 + o as u64,
                        length: s as u64,
                    },
                );
            }
        }
        log::trace!("Parsing user package files - complete");
        Ok(files)
    }

    pub async fn parse_segment_metadata<Reader>(
        &self,
        mut file: Reader,
        segment_metadata: &UserPackageFile,
    ) -> Result<HashMap<String, SegmentFile>, Box<dyn std::error::Error + Send + Sync>>
    where
        Reader: AsyncRead + AsyncSeek + Unpin,
    {
        log::trace!("Parsing segment metadata");

        file.seek(SeekFrom::Start(segment_metadata.offset)).await?;
        let segment_header = XvdSegmentMetadataHeader::read(&mut file).await?;
        let paths_offset =
            segment_header.header_length as u64 + segment_header.segment_count as u64 * 0x10;

        let mut segments = vec![];
        for _ in 0..segment_header.segment_count {
            let segment = XvdSegmentMetadataSegment::read(&mut file).await?;
            segments.push(segment);
        }

        let mut files = HashMap::new();
        let mut buf = vec![0u16; u16::MAX as usize];

        for section in &self.encrypted_regions {
            let segment_page_start = section.offset.div_ceil(PAGE_SIZE as u64);
            let mut page_offset = segment_page_start;
            for segment_no in section.first_segment_index..segment_header.segment_count {
                let segment = &segments[segment_no as usize];
                let s = segment.path_length;

                file.seek(SeekFrom::Start(
                    segment_metadata.offset + paths_offset + segment.path_offset as u64,
                ))
                .await?;
                file.read_exact(buf.as_mut_bytes()).await?;
                let file_name: String = String::from_utf16(&buf[..s as usize]).unwrap();
                let page_length = if segment.filesize == 0 {
                    1
                } else {
                    segment.filesize.div_ceil(PAGE_SIZE as u64)
                };
                if page_offset * (PAGE_SIZE as u64) >= section.offset + section.length {
                    break;
                }
                files.insert(
                    file_name,
                    SegmentFile {
                        offset: page_offset * PAGE_SIZE as u64,
                        length: segment.filesize,
                        data_hashs: vec![],
                        keep_encrypted: segment
                            .flags
                            .contains(XvdSegmentMetadataSegmentFlags::KEEP_ENCRYPTED_ON_DISK),
                    },
                );
                page_offset += page_length;
            }
        }
        log::trace!("Parsing segment metadata - complete");
        Ok(files)
    }

    pub async fn parse_ntfs_segment_metadata<Reader>(
        &self,
        file: Reader,
        only_plain: bool,
    ) -> Result<HashMap<String, SegmentFile>, Box<dyn std::error::Error + Send + Sync>>
    where
        Reader: AsyncRead + AsyncSeek + Unpin,
    {
        log::trace!("Parsing NTFS segment metadata");

        let drive_data_offset = self.drive_data_offset;
        let drive_size = self.header.drive_size;
        let drive_plain_len = self.non_encrypted_prefix_len(drive_data_offset, drive_size);

        block_in_place(|| {
            let block_size = 4096;
            let drive = SyncSubstream::new(
                XvdStream {
                    inner: SyncIoBridge::new(file),
                    offset: drive_data_offset,
                    end_offset: drive_data_offset + drive_plain_len,
                },
                0,
                drive_plain_len,
            );

            let gp = gpt::GptConfig::new()
                .writable(false)
                .logical_block_size(if block_size == 512 {
                    gpt::disk::LogicalBlockSize::Lb512
                } else if block_size == 4096 {
                    gpt::disk::LogicalBlockSize::Lb4096
                } else {
                    todo!("unsupported block_size: {}", block_size)
                })
                .open_from_device(drive)?;

            let (_, part) = gp
                .partitions()
                .iter()
                .find(|(_, part)| part.is_used())
                .ok_or_else(|| {
                    io::Error::new(ErrorKind::NotFound, "no used GPT partition found")
                })?;

            let part_start = part.bytes_start(*gp.logical_block_size()).unwrap();
            let part_len = part.bytes_len(*gp.logical_block_size()).unwrap();

            let bridge = gp.take_device().into_inner().into_inner();
            let partition_offset = drive_data_offset + part_start;
            let partition_plain_len = self.non_encrypted_prefix_len(partition_offset, part_len);
            let mut fs = SyncSubstream::new(
                XvdStream {
                    inner: bridge,
                    offset: partition_offset,
                    end_offset: partition_offset + partition_plain_len,
                },
                0,
                partition_plain_len,
            );

            let reports = collect_ntfs_stream_layouts(&mut fs)?;
            let mut files = HashMap::new();

            for report in reports {
                if report.path.starts_with('$') || report.path.contains(':') {
                    continue;
                }
                if report.resident_data || report.data_runs.len() != 1 {
                    continue;
                }

                let Some(data_run) = report.data_runs.first() else {
                    continue;
                };
                let Some(start) = data_run.start else {
                    continue;
                };

                if only_plain && partition_offset + start >= drive_data_offset + drive_plain_len {
                    continue;
                }

                files.insert(
                    report.path.replace("/", "\\"),
                    SegmentFile {
                        offset: partition_offset + start,
                        length: report.value_length,
                        data_hashs: vec![],
                        keep_encrypted: !only_plain
                            && report.path.to_ascii_lowercase().ends_with(".exe"),
                    },
                );
            }

            log::trace!("Parsing NTFS segment metadata - complete");

            Ok(files)
        })
    }

    async fn extract_file_ex<Writer, Reader, Progress>(
        &self,
        i: &mut Reader,
        out: &mut Writer,
        sfile: &SegmentFile,
        full_key: [u8; 32],
        mut progress: Progress,
        decrypt_all: bool,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Reader: AsyncRead + Unpin,
        Writer: AsyncWrite + Unpin,
        Progress: FnMut(u64, u64),
    {
        if sfile.length == 0 {
            return Ok(());
        }

        let s = &self
            .encrypted_regions
            .iter()
            .find(|s| sfile.offset >= s.offset && sfile.offset < s.offset + s.length);

        let mut tweak = None;
        let mut tweak_cipher = None;
        let mut data_cipher = None;

        let file_offset_in_section;

        if let Some(s) = s
            && (!sfile.keep_encrypted || decrypt_all)
        {
            let mut tweak_key = [0u8; 16];
            let mut data_key = [0u8; 16];
            tweak_key.copy_from_slice(&full_key[..16]);
            data_key.copy_from_slice(&full_key[16..]);

            tweak = Some(Tweak::new(
                0,
                s.region_id,
                self.header.vduid.to_bytes_le()[..8].try_into().unwrap(),
            ));
            tweak_cipher = Some(Aes128::new((&tweak_key).into()));
            data_cipher = Some(Aes128::new((&data_key).into()));
            file_offset_in_section = sfile.offset - s.offset;
        } else {
            // TODO for data integrity we need a section for unencrypted sections...
            file_offset_in_section = sfile.offset;
        }
        let page_start = file_offset_in_section / PAGE_SIZE as u64;
        let page_count = sfile.length.div_ceil(PAGE_SIZE as u64);

        let mut page = [0u8; PAGE_SIZE];

        for page_in_section in page_start..page_start + page_count {
            progress(
                min((page_in_section - page_start) * 4096, sfile.length),
                sfile.length,
            );
            i.read_exact(&mut page).await?;
            let to_write = min(
                PAGE_SIZE,
                sfile.length as usize
                    - min(
                        (page_in_section - page_start) as usize * 4096_usize,
                        sfile.length as usize,
                    ),
            ) as usize;
            let to_write = if let Some(tweak) = tweak.as_mut() {
                // tweak.update_data_unit(match &s.unwrap().data_units {
                //     Some(units) => *units.get(page_in_section as usize).ok_or_else(|| {
                //         io::Error::new(
                //             io::ErrorKind::InvalidInput,
                //             format!(
                //                 "{} units {} page_in_section {} ({}+{})",
                //                 "missing data unit",
                //                 (*units).len(),
                //                 page_in_section,
                //                 page_start,
                //                 page_count
                //             ),
                //         )
                //     })?,
                //     None => page_in_section as u32,
                // });
                // decrypt_page_xts(
                //     &mut page,
                //     *tweak,
                //     tweak_cipher.as_ref().unwrap(),
                //     data_cipher.as_ref().unwrap(),
                // );
                todo!("Tweak updating not implemented");
                to_write
            } else if sfile.keep_encrypted {
                // Decryption needs full 4k blocks
                PAGE_SIZE
            } else {
                to_write
            };
            while let Err(err) = out.write_all(&page[..to_write]).await {
                eprintln!("Error write file {} waiting 30s", err);
                println!("Error write file {} waiting 30s", err);
                sleep(tokio::time::Duration::from_secs(30)).await;
            }
        }
        Ok(())
    }

    // Reader is an full xvd file
    pub async fn extract_file<Writer, Reader, Progress>(
        &self,
        i: &mut Reader,
        out: &mut Writer,
        sfile: &SegmentFile,
        full_key: [u8; 32],
        progress: Progress,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Reader: AsyncRead + AsyncSeek + Unpin,
        Writer: AsyncWrite + Unpin,
        Progress: FnMut(u64, u64),
    {
        i.seek(std::io::SeekFrom::Start(sfile.offset)).await?;
        self.extract_file_ex(i, out, sfile, full_key, progress, false)
            .await
    }

    // Reader points to file content
    pub async fn mount_mem_fd<Writer, Reader, Progress>(
        &self,
        i: &mut Reader,
        out: &mut Writer,
        sfile: &SegmentFile,
        full_key: [u8; 32],
        progress: Progress,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Reader: AsyncRead + Unpin,
        Writer: AsyncWrite + Unpin,
        Progress: FnMut(u64, u64),
    {
        self.extract_file_ex(i, out, sfile, full_key, progress, true)
            .await
    }
}
