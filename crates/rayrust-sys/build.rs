use std::path::PathBuf;

fn main() {
    // ── Locate Ray C++ SDK ──────────────────────────────────────
    // Priority:
    //   1. RAY_CPP_DIR env var
    //   2. Auto-detect from `python3 -c "import ray; ..."`
    //   3. Common fallback paths
    let ray_cpp_dir = std::env::var("RAY_CPP_DIR").ok().or_else(|| {
        let output = std::process::Command::new("python3")
            .args([
                "-c",
                "import ray, os, sys\n\
                 for p in sys.path:\n\
                 \x20   c = os.path.join(p, 'ray', 'cpp')\n\
                 \x20   h = os.path.join(c, 'include', 'ray', 'api.h')\n\
                 \x20   l = os.path.join(c, 'lib', 'libray_api.so')\n\
                 \x20   if os.path.exists(h) and os.path.exists(l):\n\
                 \x20       print(c); break\n",
            ])
            .output()
            .ok()?;
        let s = String::from_utf8(output.stdout).ok()?;
        let s = s.trim();
        if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        }
    });

    let ray_cpp_dir = ray_cpp_dir.unwrap_or_else(|| {
        eprintln!("cargo:warning=RAY_CPP_DIR not set and auto-detect failed. Set RAY_CPP_DIR to the ray[cpp] directory.");
        eprintln!("cargo:warning=Example: RAY_CPP_DIR=/path/to/site-packages/ray/cpp cargo build");
        String::new()
    });

    let wrapper_dir = PathBuf::from("wrapper");

    // ── Compile the C ABI wrapper (.cc) ────────────────────────
    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++17")
        .file(wrapper_dir.join("ray_c.cc"))
        .include(wrapper_dir);

    if !ray_cpp_dir.is_empty() {
        let ray_cpp = PathBuf::from(&ray_cpp_dir);
        build.include(ray_cpp.join("include"));
    }

    // Warnings are non-fatal
    build.warnings(false);

    // Ray C++ SDK (libray_api.so) is built with Bazel which sets
    // _GLIBCXX_USE_CXX11_ABI=0 for maximum compatibility. We must match.
    build.define("_GLIBCXX_USE_CXX11_ABI", "0");

    build.compile("ray_c");

    // ── Link against libray_api.so ─────────────────────────────
    if !ray_cpp_dir.is_empty() {
        let lib_dir = PathBuf::from(&ray_cpp_dir).join("lib");
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
    }
    println!("cargo:rustc-link-lib=dylib=ray_api");

    // ── Tell cargo where to find the wrapper header ─────────────
    println!("cargo:rerun-if-changed=wrapper/ray_c.h");
    println!("cargo:rerun-if-changed=wrapper/ray_c.cc");
}
