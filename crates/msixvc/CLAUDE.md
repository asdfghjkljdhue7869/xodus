# CLAUDE.md — crates/msixvc

Scope: this file applies to `crates/msixvc` only. See the repo-root `CLAUDE.md` for workspace-wide build/test
commands and the other crates.

## What this is

Standalone `[rlib]` for parsing/decrypting the MSIXVC package format: XVD (`models/xvd/`, `xvd.rs`) and XSP
(`models/xsp/`, `xsp.rs`) containers, plus streaming an NTFS filesystem out of a GPT-partitioned XVD drive image
(`streaming.rs`, `streaming_ntfs.rs`) without buffering the whole package to disk. Has **no dependency on
`xodus`** — it doesn't know about MSA/Xbox Live auth, tokens, or licensing; it only turns bytes + a content key
into files. Keep it that way: if something here starts needing auth/session state, that's a sign the logic
belongs in `xodus::licensing` instead, not a reason to add the dependency.

## Module map

- `models/xvd/`, `models/xsp/` — wire formats. Each splits `structs/raw.rs` (on-disk layout) from
  `structs/parsed.rs` (validated/ergonomic form) plus a `TryFrom<raw::X> for parsed::X`. Use the
  `sync-raw-parsed-struct` skill when adding or changing a field here — it's the authoritative checklist, not
  duplicated below.
- `math.rs` — pure offset/page/hash-tree-block arithmetic (`PAGE_SIZE`-relative conversions, the hash-tree
  block/run calculation used to locate a page's integrity hash). No I/O; this is where the XVD hash-tree layout
  logic (levels, `PAGES_PER_BLOCK`) is reconstructed.
- `crypt.rs` — XTS-AES for XVD encrypted sections: `Tweak` (built from `data_unit` + `XvcRegionId` + first 8
  bytes of `vduid`, see `docs/xodus/licenses.md` for where the 32-byte content key it consumes comes from) and
  `SectionReader`, a page-cached `Read`-like decryptor over a section.
- `streaming.rs` — `HttpRead`, an `AsyncRead + AsyncSeek` over HTTP range requests (used to stream package
  content without downloading it fully first).
- `streaming_ntfs.rs` — walks an NTFS `$MFT` to report data-run layouts (`collect_ntfs_stream_layouts`) without
  extracting files, so segment metadata can be matched to byte ranges in the still-encrypted stream.
- `xvd.rs` / `xsp.rs` — the container-level readers tying the above together: `XvdFile::parse` walks the header
  → XVC region table → hash tree to build `EncryptedSectionInfo`s, then `parse_ntfs_segment_metadata` /
  `download_file_http` use those to decrypt and stream individual files out of the drive image.

## Conventions

- Raw structs: `#[repr(C, packed)]`, `derive(FromBytes)` (zerocopy), and **every multi-byte integer field must
  use zerocopy's endian-aware types** (`U16`/`U32`/`U64`/`I64` from `zerocopy::little_endian`), never bare `u32`
  etc. — see the comment at the top of `models/xvd/structs/raw.rs`. This is what makes `FromBytes` sound on a
  packed little-endian layout regardless of host endianness.
- Parsed structs use real Rust types (`chrono::DateTime`, `uuid::Uuid`, `bitflags`-derived flag types,
  `num_enum`-derived enums) and are built via `TryFrom<raw::X>`, returning a `thiserror` enum for the fallible
  cases (bad magic, unknown enum discriminant). Shared raw→parsed helpers live in `common.rs`
  (`microsoft_filetime`) and `models/common.rs` (`Version`).
- Sync/async bridging: the `gpt` and `ntfs` crates only take synchronous `Read + Seek`, but everything else here
  is `tokio` async. `SyncSubstream`/`XvdStream` (in `xvd.rs`) are the sync adapters; `SyncIoBridge` +
  `tokio::task::block_in_place` is the pattern used to cross into them from async code — don't reach for
  `block_on` here, it'll deadlock inside the tokio runtime.
- Several `todo!()`s are intentional stubs for cases not yet observed in real packages (e.g. `KeyID` other than
  0/unencrypted in `xvd.rs`, reading across an encrypted-section boundary in one call). Don't replace them with
  silent best-effort handling — if you hit one, it means a new package shape needs reverse-engineering first.

## Tests

Colocated `#[test]` modules (no separate test crate) — currently in `math.rs` and `models/common.rs`. Run with
`cargo test -p msixvc <module_name>`, e.g. `cargo test -p msixvc math`.
