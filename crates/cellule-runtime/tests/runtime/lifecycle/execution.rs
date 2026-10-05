//! Dispatch, compaction, and command/query/resolve semantics.

use super::*;

mod admission;
mod capacity;
mod compaction_fairness;
mod dispatcher;
mod handlers;
mod resolve;
mod shutdown;
