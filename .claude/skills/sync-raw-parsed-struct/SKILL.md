---
name: sync-raw-parsed-struct
description: Checklist for adding or changing a field in xodus's MSIXVC/XVD or XSP wire-format structs (crates/msixvc/src/models/{xvd,xsp}/structs/{raw,parsed}.rs), keeping the raw (on-disk) and parsed (ergonomic) struct pair, their TryFrom conversion, size constants, and downstream consumers all in sync. Use when asked to "add a field to the XVD/XSP header", "parse a new MSIXVC field", or when changing anything under msixvc's raw.rs/parsed.rs struct pairs.
---

# Sync a raw/parsed struct pair in msixvc

## Why this needs a checklist

Every wire struct in `crates/msixvc/src/models/{xvd,xsp}/structs/` exists twice: `raw.rs` (exact on-disk layout,
`#[repr(C, packed)]`, zerocopy little-endian types) and `parsed.rs` (ergonomic types — `u32`, `Uuid`, `bitflags`,
`num_enum` enums). `common.rs`'s `impl_struct!($parsed)` macro (invoked once per pair in `xvd/structs.rs` /
`xsp/structs.rs`) generates `RAW_SIZE = size_of::<raw::X>()` plus `from_array`/`from_slice`/`read` via
`zerocopy::transmute!` + `X::try_from(raw)`.

**The only compiler-enforced link between `raw::X` and `parsed::X` is that they share a name.** Nothing stops an
added raw field from being silently unused in the `TryFrom` conversion, and nothing stops size/offset drift beyond
what `RAW_SIZE`'s `size_of` picks up automatically. Treat every field addition as a manual, multi-file edit.

## Checklist

1. **Add the field to `raw::X`** in `raw.rs`, using a zerocopy little-endian type (`U16`/`U32`/`U64`/arrays
   thereof) for multi-byte integers, or a fixed `[u8; N]` for blobs/UUIDs/hashes. Keep `#[repr(C, packed)]` field
   **order** exactly matching the real on-disk layout — a misordered or missized field silently shifts every
   field after it, with no compile error.

2. **Add the corresponding field to `parsed::X`** in `parsed.rs`, using the ergonomic type:
   - plain integer via `.get()` on the zerocopy wrapper
   - `Uuid::from_bytes_le(...)` for UUID bytes
   - a `bitflags` type from `flags.rs` for bitfields (`from_bits_retain` to tolerate unknown bits)
   - a `num_enum` type from `enums.rs` for type codes (`TryFromPrimitive`/`FromPrimitive`)
   - `Version::from_fields(...)` (in `common.rs`) for 4×u16 version arrays
   - `chrono` via the `microsoft_filetime` helper for timestamps

   Create any new flag bits or enum variants in `flags.rs`/`enums.rs` first if the field needs one. Parsed doesn't
   have to mirror raw structurally — e.g. `parsed::XspPatchRecord` is a raw `flag` byte turned into an enum via
   `match` inside its `TryFrom` impl. Restructuring in the conversion is fine as long as step 3 covers it.

3. **Update the `TryFrom<raw::X>` / `From<raw::X>` impl**, defined right below the struct in the same `parsed.rs`
   file, to actually populate the new field. This is the step most likely to be silently skipped — double-check
   the struct literal includes it, since an unused raw field or a parsed field left at a stale default won't
   necessarily fail to compile.

4. **Check size constants in `constants.rs`** (e.g. `XVD_HEADER_SIZE`, `HEADER_SIGNATURE_SIZE`, `PAGE_SIZE`,
   `HASH_ENTRY_LENGTH`) if the field changes the struct's total size. These are hand-maintained, **not** derived
   from `RAW_SIZE` — `impl_struct!`'s `size_of::<raw::X>()` only covers the struct's own byte count, not any
   related constant used elsewhere for offset math.

5. **Update offset/layout helpers** if the field affects layout math: methods like `XvdHeader`'s
   `mdu_offset`/`hash_tree_info`/`xvc_info_offset` in `parsed.rs`, and any hardcoded offset arithmetic in
   `crates/msixvc/src/xvd.rs` / `xsp.rs` that reads a `RAW_SIZE` constant directly (e.g. `xvd.rs` lines doing
   offset math around header/region/hash-entry reads).

6. **Grep for other consumers** — `crates/msixvc/src/xvd.rs`, `crates/msixvc/src/xsp.rs`, and
   `crates/xodus-cli/src/commands/{extract,streaming}.rs` — for the struct/field/enum name, to catch match arms,
   flag checks, or size arithmetic that also needs updating. CLI commands generally only touch the high-level
   `XvdFile`/`SegmentFile` API, so blast radius there is usually low unless the new field needs surfacing in a
   command's output.

7. **Add or extend a round-trip test.** There is currently no raw↔parsed round-trip test anywhere in `msixvc` —
   the only existing test near this code is `models/common.rs`'s `test_version_cmp` (unrelated, just `Version`
   ordering). Model a new test on that module: build a raw byte array by hand, run it through
   `X::from_array`/`from_slice`, and assert the new parsed field comes out correctly. Then run:

   ```bash
   cargo test -p msixvc
   ```

   Treat this as filling a real gap in the safety net, not optional busywork — right now nothing but manual
   review catches a raw/parsed mismatch.
