//! The single error type for the whole crate.
//!
//! Kept as one flat enum so every callsite can `?` freely. Variants carry
//! enough context (page ids, expected/actual values) that a log line or a
//! `Display` rendering pinpoints the failure without needing a backtrace.

use crate::types::PageId;

/// Errors returned by the onyx-metadb public and internal API.
#[derive(thiserror::Error, Debug)]
pub enum MetaDbError {
    /// Passthrough for underlying `std::io` failures (file missing, disk
    /// full, etc.).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The CRC stored in a page's header does not match the computed CRC.
    /// Raised by [`crate::page::Page::verify`].
    #[error("page {page_id} failed CRC check: expected {expected:#010x}, got {actual:#010x}")]
    PageChecksumMismatch {
        page_id: PageId,
        expected: u32,
        actual: u32,
    },

    /// A page does not carry the `ONXP` magic bytes. Usually means the page
    /// was never written, or the file was truncated/overwritten.
    #[error("invalid page magic at page {page_id}: {found:#010x}")]
    PageMagicMismatch { page_id: PageId, found: u32 },

    /// A page's version byte is not recognized by this build.
    #[error("unsupported page version at page {page_id}: {version}")]
    PageVersionUnsupported { page_id: PageId, version: u8 },

    /// A page's type byte is not recognized by this build.
    #[error("unknown page type byte {0}")]
    UnknownPageType(u8),

    /// The requested page id is beyond the current high-water mark.
    #[error("page {0} is out of range for the current page store")]
    PageOutOfRange(PageId),

    /// The page store has run out of addressable pages.
    #[error("page store out of space")]
    OutOfSpace,

    /// A fixed-capacity backing device cannot satisfy a page allocation:
    /// the request would cross `capacity_pages`. Distinct from
    /// [`OutOfSpace`](Self::OutOfSpace) (address-space exhaustion) — this
    /// is a physical capacity wall on a device-backed page store, and it
    /// must abort the in-flight checkpoint cleanly (roll back, leave the
    /// prior manifest generation intact) rather than corrupt.
    #[error(
        "meta device capacity exhausted: need {requested_pages} pages, capacity {capacity_pages}"
    )]
    CapacityExhausted {
        requested_pages: u64,
        capacity_pages: u64,
    },

    /// Invalid or inconsistent argument passed to the API.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A durable-state invariant does not hold. Caller cannot recover;
    /// database is presumed corrupt.
    #[error("corruption: {0}")]
    Corruption(String),

    /// Raised only by the fault-injection framework under the
    /// `fault-injection` feature. Never returned in production builds.
    #[error("fault injected at {0}")]
    InjectedFault(&'static str),
}

impl MetaDbError {
    /// True for a transient underlying device IO failure ([`Self::Io`]). A
    /// chunklet member EIO on a *redundant* LD (RAID10/5/6) surfaces here, and
    /// is recoverable once the bad PD is isolated and the LD reopens degraded —
    /// as opposed to the fatal, restart-required classes.
    ///
    /// NOTE: this is a *classification* helper for callers. It does NOT change
    /// metadb's own sync policy: a sync-cycle failure still fails hard (a
    /// half-applied sync can leave deferred apply state inconsistent — see
    /// `Db::poison_sync`), so metadb relies on the backend (chunklet inline
    /// degrade) to keep a single member EIO from ever reaching the sync path.
    pub fn is_transient_io(&self) -> bool {
        matches!(self, MetaDbError::Io(_))
    }

    /// True for a failure the database cannot recover from in place — presumed
    /// corruption, a physical capacity wall, or a structural page error. The
    /// caller must abort / roll back / restart rather than retry.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            MetaDbError::Corruption(_)
                | MetaDbError::CapacityExhausted { .. }
                | MetaDbError::OutOfSpace
                | MetaDbError::PageChecksumMismatch { .. }
                | MetaDbError::PageMagicMismatch { .. }
                | MetaDbError::PageVersionUnsupported { .. }
                | MetaDbError::UnknownPageType(_)
                | MetaDbError::PageOutOfRange(_)
        )
    }
}

/// Shorthand for `std::result::Result<T, MetaDbError>`.
pub type Result<T> = std::result::Result<T, MetaDbError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_io_vs_fatal_classification() {
        // A device IO error (where a chunklet member EIO lands) is transient.
        let io = MetaDbError::Io(std::io::Error::from_raw_os_error(5));
        assert!(io.is_transient_io());
        assert!(!io.is_fatal());

        // Corruption / capacity / page-structure errors are fatal, not transient.
        for e in [
            MetaDbError::Corruption("bad".into()),
            MetaDbError::CapacityExhausted {
                requested_pages: 2,
                capacity_pages: 1,
            },
            MetaDbError::OutOfSpace,
            MetaDbError::PageMagicMismatch {
                page_id: 1,
                found: 0,
            },
        ] {
            assert!(e.is_fatal(), "{e} should be fatal");
            assert!(!e.is_transient_io(), "{e} is not transient IO");
        }
    }
}
