fn main() {
    // Locate ray[cpp] — same logic as rayrust-sys/build.rs
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
        if s.is_empty() { None } else { Some(s.to_string()) }
    });

    // Also check cached SDK (same cache as rayrust-sys/build.rs)
    let ray_cpp_dir = ray_cpp_dir.or_else(|| {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let cache_dir = std::path::PathBuf::from(&home)
            .join(".cache")
            .join("rayrust");
        let dir = cache_dir.join("ray-cpp-2.51.1");
        let lib = dir.join("lib").join("libray_api.so");
        if lib.exists() {
            return Some(dir.to_string_lossy().to_string());
        }
        None
    });

    if let Some(ref dir) = ray_cpp_dir {
        let lib_dir = std::path::PathBuf::from(dir).join("lib");
        println!("cargo:rustc-link-search=native={}", lib_dir.display());

        // ── RPATH: ensure librayrust_worker.so can find libray_api.so at runtime ──
        //
        // Problem: Ray's default_worker dlopens librayrust_worker.so. The dynamic
        // linker searches for NEEDED libs using RPATH/RUNPATH of the loaded .so,
        // NOT the calling process. Without RPATH, libray_api.so is not found.
        //
        // Solution: two RPATH entries, searched in order:
        //   1. $ORIGIN — .so's own directory (deployment-friendly: copy
        //      libray_api.so next to librayrust_worker.so)
        //   2. absolute path to ray/cpp/lib at build time (dev-friendly:
        //      same machine builds and runs)
        //
        // We use DT_RPATH (not DT_RUNPATH) via --disable-new-dtags because
        // RPATH is inherited by transitive dlopen dependencies, while RUNPATH
        // is only searched for direct NEEDED deps. This matters because
        // default_worker dlopens librayrust_worker.so.
        println!("cargo:rustc-link-arg-cdylib=-Wl,--disable-new-dtags");
        println!("cargo:rustc-link-arg-cdylib=-Wl,-rpath,$ORIGIN");
        println!("cargo:rustc-link-arg-cdylib=-Wl,-rpath,{}", lib_dir.display());
    }

    // Link against libray_api.so and force it into NEEDED.
    // Without --no-as-needed, the linker may drop libray_api.so from
    // the NEEDED list because no Rust code directly references its symbols.
    println!("cargo:rustc-link-arg-cdylib=-Wl,--no-as-needed");
    println!("cargo:rustc-link-arg-cdylib=-lray_api");
    println!("cargo:rustc-link-arg-cdylib=-Wl,--as-needed");
}
