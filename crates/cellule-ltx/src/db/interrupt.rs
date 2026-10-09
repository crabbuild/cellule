//! Interruption covers both serialized owner SQL connections.

/// Thread-safe interruption of a managed database's writer and owner reader.
///
/// The owner still awaits the dispatched operation and reconciles its outcome.
/// Interrupting an idle connection has no effect on a subsequent operation;
/// both underlying handles become inert after their connections close.
pub struct DbInterruptHandle {
    pub(super) writer: rusqlite::InterruptHandle,
    pub(super) reader: rusqlite::InterruptHandle,
}

impl DbInterruptHandle {
    /// Interrupts any currently executing owner command or read.
    pub fn interrupt(&self) {
        self.writer.interrupt();
        self.reader.interrupt();
    }
}
