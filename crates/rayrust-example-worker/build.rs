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

    if let Some(ref dir) = ray_cpp_dir {
        let lib_dir = std::path::PathBuf::from(dir).join("lib");
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
    }

    // Link against libray_api.so and force it into NEEDED.
    // Without --no-as-needed, the linker may drop libray_api.so from
    // the NEEDED list because no Rust code directly references its symbols.
    println!("cargo:rustc-link-arg-cdylib=-Wl,--no-as-needed");
    println!("cargo:rustc-link-arg-cdylib=-lray_api");
    println!("cargo:rustc-link-arg-cdylib=-Wl,--as-needed");
}
