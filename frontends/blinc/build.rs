use std::fs;
use std::path::Path;

use base64::Engine;

fn main() {
    let assets_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../assets/images");
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest = Path::new(&out_dir).join("embedded_icons.rs");

    let mut code = String::from("&[\n");

    if let Ok(entries) = fs::read_dir(&assets_dir) {
        let mut entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some(ext) = path.extension().and_then(|s| s.to_str()) else {
                continue;
            };
            let mime = match ext {
                "png" => "image/png",
                "ico" => "image/x-icon",
                "svg" => "image/svg+xml",
                "jpg" | "jpeg" => "image/jpeg",
                _ => continue,
            };

            let bytes = fs::read(&path).unwrap();
            let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
            let data_uri = format!("data:{mime};base64,{b64}");

            code.push_str(&format!("    (\"{stem}\", \"{data_uri}\"),\n"));
        }
    }

    code.push_str("]\n");
    fs::write(dest, code).unwrap();

    println!("cargo:rerun-if-changed=../../assets/images");
}
