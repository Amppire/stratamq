//! Captures the commit the binary is built from, so reports record the build
//! commit rather than whatever checkout the binary later runs in.

use std::process::Command;

fn main() {
    let sha = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=STRATA_BENCH_GIT_SHA={sha}");

    // Rebuild when the checked-out commit changes.
    for name in ["HEAD", "refs"] {
        if let Some(path) = git(&["rev-parse", "--git-path", name]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    println!("cargo:rerun-if-changed=build.rs");
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8(out.stdout).ok()?.trim().to_owned())
}
