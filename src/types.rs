//! Core identifier types and sentinel constants.
//!
//! All id types are plain integer aliases so they cost nothing at runtime
//! and pass by value freely. Strong typing (newtype wrappers) may come in a
//! later phase if type confusion becomes a real problem; today it would be
//! churn for no benefit.

/// Monotonically-increasing log sequence number. Every committed WAL record
/// is assigned a unique LSN, and each page carries the LSN at which it was
/// most recently written in its header (`generation`).
pub type Lsn = u64;

/// Index of a 4 KiB page within the page file.  Page 0 and page 1 are the
/// double-buffered manifest slots; data pages start at [`FIRST_DATA_PAGE`].
pub type PageId = u64;

/// Onyx logical block address. 4 KiB-sized units in the user-facing address
/// space of a volume.
pub type Lba = u64;

/// Onyx physical block address. 4 KiB-sized units in the data-plane backing
/// store. Tracked by the dedup / refcount indexes.
pub type Pba = u64;

/// Stable identifier for a partition. One partition maps to one Onyx volume.
pub type PartitionId = u32;

/// Snapshot identifier, unique per `Db`. Assigned monotonically at snapshot
/// creation time.
pub type SnapshotId = u64;

/// Sentinel used inside internal B+tree pages to mean "no child". Chosen at
/// the high end of the address space so that a stray 0 (unwritten memory)
/// cannot be mistaken for a valid page id.
pub const NULL_PAGE: PageId = u64::MAX;

/// Manifest slot A: holds one half of the double-buffered manifest.
pub const MANIFEST_PAGE_A: PageId = 0;

/// Manifest slot B: holds the other half of the double-buffered manifest.
pub const MANIFEST_PAGE_B: PageId = 1;

/// First page id available for data (B+tree, LSM, free-list-node). Pages
/// below this are reserved for the manifest.
pub const FIRST_DATA_PAGE: PageId = 2;
