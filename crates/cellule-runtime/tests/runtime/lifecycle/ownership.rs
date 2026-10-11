//! Leases, fencing, takeover, and successor acquisition.

use super::*;

mod follower_process;
mod lease;
mod partition_proxy;
mod prefix;
mod recovery;
mod sparse_process;
mod succession;
mod takeover;
