# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Xodus reimplements the Microsoft/Xbox authentication and content-delivery pipeline (MSA/Xbox Live login, device
provisioning, licensing, MSIXVC package download/decryption) so Xbox PC (GDK) games can be run on Linux/Mac. Most
of the codebase is a from-scratch client for undocumented Microsoft SOAP/XML and REST protocols reconstructed via
reverse engineering — see `docs/xodus/` and `docs/xbox/` before touching auth or licensing code, they document the
*why* behind non-obvious protocol choices that aren't visible from the code alone.

> [!CAUTION]
> Unofficial project, not affiliated with Microsoft/Xbox. Be mindful that most non-GUI crates talk to real
> Microsoft/Xbox Live endpoints and handle real account credentials/tokens.

## Workspace layout

Cargo workspace, `resolver = "3"`, members are `crates/*`:

- `crates/msixvc` — `[rlib]` standalone crate for parsing/decrypting MSIXVC and XSP package formats, plus NTFS/GPT
  streaming (`streaming.rs`, `streaming_ntfs.rs`). Has no dependency on `xodus`; models live under `src/models/xvd`
  and `src/models/xsp` split into `raw` (on-disk layout) vs `parsed` (usable) structs.
- `crates/xodus` — `[rlib]` core: MSA/Xbox auth (`api/`, `auth.rs`, `tokens/`), device provisioning (`hardware.rs`,
  `clep/`), licensing/CIK decryption (`licensing/`), and the credential store (`tokens/`, `secrets.rs`). Depends on
  the external `xal` crate (pinned by git rev) for lower-level XAL/XASU primitives, and compiles `proto/xodus/common.proto`
  via `prost-build` in `build.rs` (requires `protoc` on `PATH`).
- `crates/xodus-cli` — `[bin]` CLI, the primary place new xodus features get exercised end-to-end. Owns the login
  webview (`webview.rs`, using `tao`/`wry`) since there's no real `CloudExperienceHost` to host the MSA login page.
- `crates/xodus-service` — `[bin]` long-running service exposing a Unix socket (`xodus.sock`, default in the
  runtime dir) for IPC. Intended to become the single integration point all Xodus clients (CLI, GUI, future
  `xgameruntime.dll` shim) talk to.

Dependencies shared across crates (`tokio`, `reqwest`, `chrono`, `base64`) are pinned once in the root
`Cargo.toml` `[workspace.dependencies]`; add new shared deps there rather than pinning divergent versions per-crate.

## Build / check / test

```bash
cargo check --workspace          # what CI runs on every PR (Linux + macOS aarch64)
cargo build --release --workspace
cargo fmt --check --all          # CI also runs this; use `cargo fmt --all` to fix
cargo test --workspace           # tests are sparse and colocated (#[test] in the file under test), no separate test crates
cargo test -p msixvc streaming   # run tests in one crate/module, e.g. msixvc::streaming
cargo run --bin xodus-cli -- --help
cargo run --bin xodus-service
```

Prerequisites: a Rust toolchain supporting `edition = "2024"`; `protoc` on `PATH` (needed by `xodus`'s `build.rs`);
on Linux, `libwebkit2gtk-4.1-dev` (for `xodus-cli`'s `wry` webview).

`.cargo/config.toml` force-enables `+aes,+ssse3` (x86_64) / `+aes` (aarch64) target features workspace-wide for
faster MSIXVC decryption — this means built binaries will `SIGILL` on CPUs without AES-NI (pre-2011 x86_64).
There's no feature-detection fallback; don't try to "fix" the crash by catching the signal, the target-feature
flags are the fix already in place, this is a known, accepted tradeoff.

CI (`.github/workflows/rust.yml`) only runs `cargo fmt --check --all` and `cargo check --workspace` — there is no
CI-enforced clippy or test job, so don't assume `cargo clippy` is clean; run it yourself if refactoring, but treat
its output as advisory.

## Architecture notes

**Token storage** (`xodus::tokens`): `TokenManager` (`tokens/manager.rs`) is the single facade both `xodus-cli`
and `xodus-service` use for credentials — don't read/write the keychain directly. It composes two tiers behind the
`TokenBackend` / `ExpiringTokenBackend` traits (`tokens/backend.rs`, `tokens/store.rs`): a persistent, OS-keychain-backed
tier (`KeychainBackend`, via `keyring-core` + platform store — `dbus-secret-service-keyring-store` on Linux,
`apple-native-keyring-store` on macOS) for device/user STS credentials, and an in-memory tier (`MemoryBackend`) for
short-lived per-relying-party XSTS tokens. `TokenManager::with_keychain_and_memory()` is the standard wiring; call
`xodus::secrets::init_secrets()` once at startup before constructing it (sets the global `keyring_core` store) and
`xodus::secrets::destroy_secrets()` on shutdown. `xodus::tokens::device::ensure_device_credentials` is the
idempotent "make sure a device identity exists and its STS token is fresh" entry point every binary calls at
startup.

**Auth/licensing protocol stack** — read `docs/xodus/README.md`'s linked pages before changing this code, they
capture non-obvious server behavior (e.g. why `TPMInfo` is intentionally omitted, why the first `RST2.srf` call
must be RSA-signed rather than password-authenticated) that isn't derivable from source alone:
- `docs/xodus/device.md` — device provisioning (`deviceaddcredential.srf`) and device STS issuance (`RST2.srf`).
- `docs/xodus/clep.md` — the two unrelated things called "CLEP": obfuscated hardware-fingerprint blobs sent in
  provisioning (`clep/challenge.rs`), vs. AES-encrypted `ClepSignState`/`ClepHmacState` secrets returned by the
  server (shared 4096-byte encoding, decrypted via `decrypt_cbc_zero_iv`, see `licensing/splicense.rs`).
- `docs/xodus/login.md` — user login via `InlineLogin.srf`. `xodus-cli/src/webview.rs` impersonates just enough of
  Windows' `CloudExperienceHost` (JS IPC bridge, `cxh-*` headers) for the real MSA login page to work unmodified;
  the `SessionHandler` trait / `RuntimeCommands` queue there is the generic webview-session runtime, with
  `commands/login.rs`'s `LoginHandler` as the only current implementation.
- `docs/xodus/licenses.md` — content licensing flow (device+user token + ContentId → SPLicenseBlock → content keys).

**xodus-service IPC** (`crates/xodus-service/src/connection/`): `router.rs` accepts Unix-socket connections, reads
a 4-byte little-endian magic (`XML_MAGIC` / `PROTO_MAGIC` in `main.rs`) per message and dispatches to `xml.rs` or
`proto.rs`. The protobuf path (`proto/xodus/common.proto`, compiled via `prost`) is a stub (`unimplemented!`) — the
XML path is the one currently in use.

**msixvc formats**: XVD (`models/xvd/`) and XSP (`models/xsp/`) each split `structs/raw.rs` (wire/on-disk layout,
`zerocopy`-derived) from `structs/parsed.rs` (validated/usable form) — when adding fields, update both and the
conversion between them, not just one.
