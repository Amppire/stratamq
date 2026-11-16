//! `strata-bench` CLI: runs a scenario and writes a JSON report.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use strata_bench::generator::{KeyDistribution, WorkloadSpec};
use strata_bench::mock::MemStorage;
use strata_bench::runner::{RunConfig, run_storage, write_report};
use strata_bench::traits::Durability;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DistributionArg {
    Uniform,
    Zipfian,
    HotKey,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DurabilityArg {
    Buffered,
    Fsync,
}

/// Runs a storage scenario against the built-in in-memory mock candidate.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// Scenario id recorded in the report.
    #[arg(long, default_value = "STORE-W1")]
    scenario: String,
    #[arg(long, value_enum, default_value = "uniform")]
    distribution: DistributionArg,
    /// Zipfian skew; only used with `--distribution zipfian`.
    #[arg(long, default_value_t = 1.0)]
    exponent: f64,
    #[arg(long, default_value_t = 10_000)]
    key_space: u64,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, default_value_t = 10_000)]
    operations: u64,
    #[arg(long, default_value_t = 128)]
    payload_bytes: usize,
    #[arg(long, value_enum, default_value = "fsync")]
    durability: DurabilityArg,
    /// Directory the JSON report is written to.
    #[arg(long, default_value = "reports")]
    output_dir: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let distribution = match args.distribution {
        DistributionArg::Uniform => KeyDistribution::Uniform,
        DistributionArg::Zipfian => KeyDistribution::Zipfian {
            exponent: args.exponent,
        },
        DistributionArg::HotKey => KeyDistribution::HotKey,
    };
    let config = RunConfig {
        scenario: args.scenario,
        implementation: "mem-storage".to_owned(),
        workload: WorkloadSpec {
            distribution,
            key_space: args.key_space,
            seed: args.seed,
        },
        operations: args.operations,
        payload_bytes: args.payload_bytes,
        durability: match args.durability {
            DurabilityArg::Buffered => Durability::Buffered,
            DurabilityArg::Fsync => Durability::Fsync,
        },
    };

    let outcome = async {
        let result = run_storage(&config, &MemStorage::default()).await?;
        write_report(&args.output_dir, &result)
    }
    .await;

    match outcome {
        Ok(path) => {
            println!("wrote {}", path.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}
