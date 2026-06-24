use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Default Ray version to download if RAY_VERSION env var is not set.
const DEFAULT_RAY_VERSION: &str = "2.51.1";

fn main() {
    // ── Locate Ray C++ SDK ──────────────────────────────────────
    // Priority:
    //   1. RAY_CPP_DIR env var (user-specified path to ray/cpp/)
    //   2. Cached download in OUT_DIR (from previous build)
    //   3. Download from PyPI wheel (auto-detect platform)
    //
    // This does NOT depend on Python being installed — it downloads the
    // Ray wheel directly from PyPI and extracts the C++ SDK (ray/cpp/).

    let ray_cpp_dir: PathBuf = match env::var("RAY_CPP_DIR")
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(dir) => {
            println!("cargo:warning=Using RAY_CPP_DIR={}", dir);
            PathBuf::from(dir)
        }
        None => {
            let version =
                env::var("RAY_VERSION").unwrap_or_else(|_| DEFAULT_RAY_VERSION.to_string());
            download_ray_cpp(&version)
        }
    };

    let wrapper_dir = PathBuf::from("wrapper");

    // ── Compile the C ABI wrapper (.cc) + worker export ────────
    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++17")
        .file(wrapper_dir.join("ray_c.cc"))
        .file(wrapper_dir.join("ray_worker_export.cc"))
        .include(wrapper_dir)
        .include(ray_cpp_dir.join("include"));

    // Warnings are non-fatal
    build.warnings(false);

    // Ray C++ SDK (libray_api.so) is built with Bazel which sets
    // _GLIBCXX_USE_CXX11_ABI=0 for maximum compatibility. We must match.
    build.define("_GLIBCXX_USE_CXX11_ABI", "0");

    build.compile("ray_c");

    // ── Link against libray_api.so ─────────────────────────────
    let lib_dir = ray_cpp_dir.join("lib");
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=ray_api");

    // ── Tell cargo where to find the wrapper header ─────────────
    println!("cargo:rerun-if-changed=wrapper/ray_c.h");
    println!("cargo:rerun-if-changed=wrapper/ray_c.cc");
    println!("cargo:rerun-if-changed=wrapper/ray_worker_export.cc");
    println!("cargo:rerun-if-changed=build.rs");
}

// ─── PyPI wheel download ─────────────────────────────────────────

/// Detect the PyPI platform tag for the current system.
fn detect_platform_tag() -> Result<&'static str, String> {
    let arch = env::consts::ARCH;
    let os = env::consts::OS;
    match (os, arch) {
        ("linux", "x86_64") => Ok("manylinux2014_x86_64"),
        ("linux", "aarch64") => Ok("manylinux2014_aarch64"),
        ("macos", "x86_64") => Ok("macosx_12_0_x86_64"),
        ("macos", "aarch64") => Ok("macosx_12_0_arm64"),
        ("windows", "x86_64") => Ok("win_amd64"),
        _ => Err(format!("unsupported platform: {}-{}", os, arch)),
    }
}

/// Fetch the wheel download URL from PyPI JSON API.
fn fetch_wheel_url(version: &str, platform: &str) -> Result<String, String> {
    let api_url = format!("https://pypi.org/pypi/ray/{}/json", version);

    let output = Command::new("curl")
        .args(["-s", "--fail", "--connect-timeout", "30", &api_url])
        .output()
        .map_err(|e| format!("curl not available or failed: {}", e))?;

    if !output.status.success() {
        return Err(format!(
            "failed to fetch PyPI metadata for ray {}: {}",
            version,
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let json = String::from_utf8_lossy(&output.stdout);

    // We try cp312 first, then cp311, cp310 — the C++ SDK is identical
    // across Python versions, so any wheel will work.
    for py_tag in &["cp312", "cp311", "cp310"] {
        let wheel_filename = format!("ray-{}-{}-{}-{}.whl", version, py_tag, py_tag, platform);
        if let Some(url) = extract_url_for_filename(&json, &wheel_filename) {
            return Ok(url);
        }
    }

    Err(format!(
        "no wheel found for ray {} on platform {}",
        version, platform
    ))
}

/// Extract the "url" field value that appears after the given filename in the JSON.
fn extract_url_for_filename(json: &str, filename: &str) -> Option<String> {
    let idx = json.find(filename)?;
    let after = &json[idx..];
    let url_key = after.find("\"url\"")?;
    let after_url = &after[url_key..];
    let colon = after_url.find(':')?;
    let after_colon = &after_url[colon + 1..];
    let q1 = after_colon.find('"')?;
    let after_quote = &after_colon[q1 + 1..];
    let q2 = after_quote.find('"')?;
    Some(after_quote[..q2].to_string())
}

/// Download the Ray wheel from PyPI and extract ray/cpp/ to a shared cache.
/// Uses ~/.cache/rayrust/ray-cpp-{version}/ so the download is shared between
/// debug and release builds (OUT_DIR differs per profile).
fn download_ray_cpp(version: &str) -> PathBuf {
    // Shared cache: ~/.cache/rayrust/ray-cpp-{version}/
    let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    let cache_root = env::var("RAYRUST_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(&home).join(".cache").join("rayrust"));
    let cache_dir = cache_root.join(format!("ray-cpp-{}", version));
    let marker = cache_dir.join(".ray_cpp_version");

    // Check cache — if the version marker matches, reuse.
    if marker.exists() {
        if let Ok(cached_version) = fs::read_to_string(&marker) {
            if cached_version.trim() == version {
                println!(
                    "cargo:warning=Using cached Ray C++ SDK {} at {}",
                    version,
                    cache_dir.display()
                );
                return cache_dir;
            }
        }
    }

    let platform = detect_platform_tag().unwrap_or_else(|e| {
        panic!(
            "Cannot auto-detect platform for Ray SDK download: {}\n\
             Set RAY_CPP_DIR to point to an existing ray/cpp/ directory.",
            e
        )
    });

    println!(
        "cargo:warning=Downloading Ray C++ SDK {} ({}) from PyPI...",
        version, platform
    );
    println!("cargo:warning=  (71MB wheel — this may take a few minutes on first build)");
    println!("cargo:warning=  cached at {} for future builds", cache_dir.display());

    let wheel_url = fetch_wheel_url(version, platform).unwrap_or_else(|e| {
        panic!(
            "Failed to find Ray {} wheel on PyPI: {}\n\
             Set RAY_CPP_DIR to point to an existing ray/cpp/ directory, or\n\
             set RAY_VERSION to a different version.",
            version, e
        )
    });

    // Download wheel to temp file in cache root
    fs::create_dir_all(&cache_root).expect("failed to create cache root");
    let wheel_path = cache_root.join(format!("ray-{}.whl", version));

    // Use --progress-bar so user sees download progress
    let status = Command::new("curl")
        .args([
            "--fail",
            "--connect-timeout",
            "60",
            "--retry",
            "3",
            "-L", // follow redirects
            "-o",
            &wheel_path.to_string_lossy(),
            &wheel_url,
        ])
        .status()
        .unwrap_or_else(|e| panic!("curl not available: {}", e));

    if !status.success() {
        panic!(
            "Failed to download Ray wheel from {}\n\
             Set RAY_CPP_DIR to point to an existing ray/cpp/ directory.",
            wheel_url
        );
    }

    // Extract ray/cpp/ from the wheel (wheel is a zip file)
    fs::remove_dir_all(&cache_dir).ok();
    fs::create_dir_all(&cache_dir).expect("failed to create cache dir");

    // Extract to a temp dir first, then move ray/cpp/* into cache_dir
    let tmp_extract = cache_root.join(format!("ray-extract-{}", version));
    fs::remove_dir_all(&tmp_extract).ok();
    fs::create_dir_all(&tmp_extract).expect("failed to create temp extract dir");

    let status = Command::new("unzip")
        .args([
            "-q",
            "-o",
            &wheel_path.to_string_lossy(),
            "ray/cpp/*",
            "-d",
            &tmp_extract.to_string_lossy(),
        ])
        .status()
        .unwrap_or_else(|e| panic!("unzip not available: {}", e));

    if !status.success() {
        panic!(
            "Failed to extract ray/cpp/ from wheel.\n\
             The wheel is at {}. You can extract it manually.",
            wheel_path.display()
        );
    }

    let extracted_cpp = tmp_extract.join("ray").join("cpp");
    if !extracted_cpp.exists() {
        panic!(
            "ray/cpp/ not found in wheel after extraction.\n\
             The wheel may not contain the C++ SDK."
        );
    }

    // Move extracted contents into cache_dir
    copy_dir_recursive(&extracted_cpp, &cache_dir);

    // Write version marker
    fs::write(&marker, version).expect("failed to write marker");

    // Clean up
    fs::remove_file(&wheel_path).ok();
    fs::remove_dir_all(&tmp_extract).ok();

    println!(
        "cargo:warning=Ray C++ SDK {} extracted to {}",
        version,
        cache_dir.display()
    );

    cache_dir
}

/// Recursively copy a directory's contents.
fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).expect("failed to create dir");
    for entry in fs::read_dir(src).expect("failed to read dir") {
        let entry = entry.expect("failed to read entry");
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path);
        } else {
            fs::copy(&src_path, &dst_path).expect("failed to copy file");
        }
    }
}
