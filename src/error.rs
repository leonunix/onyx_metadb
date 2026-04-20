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

/// Shorthand for `std::result::Result<T, MetaDbError>`.
pub type Result<T> = std::result::Result<T, MetaDbError>;
