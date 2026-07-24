---
name: decode-secret-blob
description: Decodes a captured base64 blob into its plaintext fields for xodus's MSA/Xbox device-secret formats (SPLicenseBlock, ClepSignState, ClepHmacState, EncryptedDeviceKey, or an obfuscated CLEP challenge). Use when asked to "decode this SPLicenseBlock", "decrypt this ClepSignState/ClepHmacState", "what's in this device secret blob", or given a captured base64 value from deviceaddcredential.srf / RST2.srf and asked what it contains.
---

# Decode a captured secret blob

Xodus deals with several unrelated base64 blob formats that look similar and are easy to conflate. Identify which
one you have before doing anything else.

## 1. Identify the blob

Base64-decode it and check the byte length plus where it was captured:

| Length | Source | What it is |
|---|---|---|
| Variable (TLV) | `deviceaddcredential.srf` response | Full `SPLicenseBlock` (contains `ClepSignState` at TLV id `0x12d`, and sometimes `EncryptedDeviceKey`) |
| Exactly 2048 bytes | `deviceaddcredential.srf` **request**, components 8196/8197 | Obfuscated `ClepV2`/`ClepV4` **challenge** — not encrypted, just XOR/Feistel-obfuscated, no key material |
| Exactly 4096 bytes, from inside an `SPLicenseBlock` | `deviceaddcredential.srf` response | `ClepSignState` |
| Exactly 4096 bytes, from `RST2.srf`'s `<wst:BinarySecret>` | first `RST2.srf` response | `ClepHmacState` |
| Exactly 4096 bytes, from `DeviceLicense` | licensing flow | `EncryptedDeviceKey` |

See `docs/xodus/clep.md` and `docs/xodus/device.md` for the full protocol context (why these exist, how the
4096-byte structures share one encoding, why the AES key comes out of `key_schedule` offsets instead of a normal
key expansion).

## 2. Naming gotcha — read this before reaching for `clep decrypt`

`xodus-cli clep decrypt` decodes the **2048-byte obfuscated challenge** blob only
(`xodus::clep::challenge::clep_deobfuscate`). Despite the name, it has nothing to do with `ClepSignState` /
`ClepHmacState` / `EncryptedDeviceKey` — those are a different, AES-encrypted format that just happens to share
the "CLEP" name. Feeding it a 4096-byte AES blob will fail (wrong length).

## 3. Decode it

**Path A — you have a full `SPLicenseBlock`:**

```bash
cargo run -p xodus-cli -- sp-license <block>
```

This parses the TLV via `SPLicense::parse_base64` (`crates/xodus/src/licensing/splicense.rs`), decrypts the
embedded `ClepSignState`, and prints the RSA key as one base64 line. It does **not** surface `EncryptedDeviceKey`
even when present in the same block — for that, or for a standalone `ClepSignState`, use Path C.

**Path B — you have a bare 2048-byte challenge blob:**

```bash
cargo run -p xodus-cli -- clep decrypt <data>
```

Prints `version`, `smbios`, `disk_serial`, and full `plaintext` fields.

**Path C — you have a bare 4096-byte `ClepSignState` / `ClepHmacState` / `EncryptedDeviceKey`:**

No CLI subcommand exists for these standalone. Write a throwaway Rust snippet in the scratchpad (do not commit
it) that links against the `xodus` lib crate. All three types and their decrypt methods are `pub` (confirmed by
`xodus-cli`'s cross-crate use of `ClepSignState::get_rsa_key()`), and all assert `version == 4` internally and
panic if it isn't — check that first if decoding fails. Template:

```rust
use base64::{Engine, engine::general_purpose::STANDARD};
use xodus::licensing::splicense::{ClepSignState, ClepHmacState, EncryptedDeviceKey};
use zerocopy::transmute;

fn main() {
    let raw = STANDARD.decode("<paste base64 here>").expect("bad base64");
    let bytes: [u8; 4096] = raw.try_into().expect("expected exactly 4096 bytes");

    // Pick ONE of the following based on what you identified in step 1:

    // ClepSignState -> 544-byte BCrypt RSA private key blob
    let state: ClepSignState = transmute!(bytes);
    let rsa_blob = state.get_rsa_key();
    println!("{}", STANDARD.encode(rsa_blob.as_bytes()));

    // ClepHmacState -> 32-byte HMAC secret (already sliced from key_data[12..44])
    // let state: ClepHmacState = transmute!(bytes);
    // let hmac_secret = state.get_hmac_state();
    // println!("{}", STANDARD.encode(hmac_secret));

    // EncryptedDeviceKey -> DeviceKey
    // let state: EncryptedDeviceKey = transmute!(bytes);
    // let device_key = state.derive_device_key();
    // println!("{:?}", device_key);
}
```

Adjust the exact method signatures against `crates/xodus/src/licensing/splicense.rs` if they've since changed.

## 4. Optional follow-on

For a `ClepSignState` RSA blob, `xodus::licensing::utils::parse_bcrypt_rsa_private` turns the raw `BCryptRsaBlock`
into a real `RsaPrivateKey` (parses the `RSA2`/`RSA3` magic, reads exponent/modulus/p/q, recomputes `d` if only
`RSA2` is present).
