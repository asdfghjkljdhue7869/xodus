use std::path::PathBuf;
use std::process::ExitCode;

use eappx::keys::{KeyCollection, KeyId};
use eappx::EAppxFile;
use uuid::Uuid;

/// Extracts a locally-stored EAppx/EMSIX package.
///
/// This is intentionally local-only for now: no store lookup or download.
/// See issue #91 - the initial goal is understanding/unpacking the format,
/// not full download integration, which can build on this once it lands.
pub async fn run(
    path: String,
    destination: String,
    key_file: Option<String>,
    key_test: bool,
) -> ExitCode {
    let infile = PathBuf::from(&path);
    let outdir = PathBuf::from(&destination);

    let file = match std::fs::File::open(&infile) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open {:?}: {e}", infile);
            return ExitCode::FAILURE;
        }
    };
    let mut reader = std::io::BufReader::new(file);

    let mut eappx = match EAppxFile::from_stream(&mut reader) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Failed to parse EAppx header: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!("{eappx}");

    let mut key_collection = KeyCollection::default();
    if let Some(key_file) = key_file {
        let mut keyfile = match std::fs::File::open(&key_file) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to open key file {:?}: {e}", key_file);
                return ExitCode::FAILURE;
            }
        };
        match KeyCollection::from_reader(&mut keyfile) {
            Ok(loaded) => key_collection.extend(loaded.keys),
            Err(e) => {
                eprintln!("Failed to parse key file: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    if key_test {
        let test_keyid = KeyId::Guid((
            Uuid::parse_str("ddafcf67-7b2c-086d-302b-8adac1bdd3a7").unwrap(),
            Uuid::parse_str("7d53aeb8-5922-f062-b1d7-7e09f5a187a0").unwrap(),
        ));
        let test_key = hex::decode(
            "9fe75f879e95a5d7f3715c30fce71067fc346efd680fa25e3c737d76acb72b9d",
        )
        .unwrap();
        key_collection.add(test_keyid, test_key);
    }

    if !key_collection.has_required_keys(&eappx.header.key_ids) {
        eprintln!("Missing one or more required content keys for this package");
        return ExitCode::FAILURE;
    }

    if let Err(e) = eappx.load_keys(&key_collection) {
        eprintln!("Failed to load keys: {e}");
        return ExitCode::FAILURE;
    }

    if !outdir.exists() {
        if let Err(e) = std::fs::create_dir_all(&outdir) {
            eprintln!("Failed to create output directory {:?}: {e}", outdir);
            return ExitCode::FAILURE;
        }
    }

    match eappx.extract(&mut reader, &outdir) {
        Ok(_) => {
            println!("Extracted {:?} to {:?}", infile, outdir);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("Extraction failed: {e}");
            ExitCode::FAILURE
        }
    }
}
