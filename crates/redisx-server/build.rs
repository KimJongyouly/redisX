//! Phase B (`redis_document/25_redisX.md`): the one genuine gap left after
//! Phase 6 proved `--features python` "just works" with zero manual setup —
//! pyo3's `auto-initialize` mode links the embedding binary directly against
//! `libpython3.x.{dylib,so}`, but on a non-framework install (Homebrew,
//! Anaconda/Miniconda, most Linux distros) that library isn't anywhere the
//! dynamic linker searches by default. Without this, running the built
//! binary needs `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH` set by hand every time
//! (confirmed the hard way during Phase 6's own validation: `dyld: Library
//! not loaded: @rpath/libpython3.13.dylib`).
//!
//! This fork's C version solved the identical problem (`CHANGE.md` §4,
//! "런타임 rpath 임베딩") by adding `-Wl,-rpath,<python-prefix>/lib` to
//! `redis-server`'s link step. Same fix here, just discovered differently:
//! `pyo3_build_config::get()` reads back the *exact* interpreter config
//! pyo3's own build script already resolved for this build (no second
//! auto-detection pass of our own to maintain or drift out of sync with
//! pyo3's), and `lib_dir` is precisely the directory containing the
//! library this binary links against.
//!
//! `-Wl,-rpath,<dir>` is accepted by both ld64 (macOS — becomes an
//! `LC_RPATH` load command) and GNU ld/lld (Linux — becomes `DT_RUNPATH`),
//! so one line covers both platforms without conditional logic. When the
//! `python` feature is off, this build script does nothing at all — the
//! default build stays exactly as dependency-free as before.
fn main() {
    #[cfg(feature = "python")]
    {
        if let Some(lib_dir) = &pyo3_build_config::get().lib_dir {
            // Not `-bins`-restricted: `cargo test`'s own test-harness binary
            // links against this crate too (its `#[cfg(test)]` modules call
            // into `script_python.rs`) and needs the same rpath to run
            // without `DYLD_LIBRARY_PATH` just as much as the real binary.
            println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
        }
    }
}
