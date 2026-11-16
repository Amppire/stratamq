//! End-to-end: run the CLI binary and check the report it writes.

use std::process::Command;

use strata_bench::schema::BenchResult;

#[test]
fn cli_writes_a_report_that_matches_the_schema() {
    let dir = std::env::temp_dir().join(format!("strata-bench-cli-{}", std::process::id()));

    let output = Command::new(env!("CARGO_BIN_EXE_strata-bench"))
        .args([
            "--operations",
            "500",
            "--distribution",
            "zipfian",
            "--output-dir",
        ])
        .arg(&dir)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    let files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(files.len(), 1);

    let result: BenchResult =
        serde_json::from_str(&std::fs::read_to_string(&files[0]).unwrap()).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();

    assert_eq!(result.scenario, "STORE-W1");
    assert_eq!(result.implementation, "mem-storage");
}

#[test]
fn cli_fails_on_an_empty_key_space() {
    let dir = std::env::temp_dir().join(format!("strata-bench-cli-bad-{}", std::process::id()));

    let output = Command::new(env!("CARGO_BIN_EXE_strata-bench"))
        .args(["--key-space", "0", "--output-dir"])
        .arg(&dir)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!dir.exists(), "no report should be written on failure");
}
