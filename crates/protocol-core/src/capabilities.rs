bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct Capabilities: u64 {
        const BATCH_PUBLISH   = 1 << 0;
        const DEMAND_WINDOW   = 1 << 1;
        const WORK_LEASES     = 1 << 2;
        const STREAM_CURSOR   = 1 << 3;
        const REPLAY_BRANCH   = 1 << 4;
        const ATOMIC_OPS      = 1 << 5;
        const COMPRESS_LZ4    = 1 << 6;
        const COMPRESS_ZSTD   = 1 << 7;
        const QUIC_TRANSPORT  = 1 << 8;
    }
}