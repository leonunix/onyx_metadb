use super::*;

/// Audit this already-open `Db` for structural consistency (orphan pages,
/// live/free conflicts, optional birth-shadow / clone-livelist checks) — the
/// `Db`-level counterpart of [`crate::verify::verify_path`] for callers that
/// already have a live handle (chunklet-backed metadb has no plain file to
/// hand `verify_path` a path to). Purely read-only: reads the current durable
/// manifest and scans `page_store`, no page store or manifest mutation.
impl Db {
    pub fn verify(
        &self,
        options: crate::verify::VerifyOptions,
    ) -> Result<crate::verify::VerifyReport> {
        let loaded = match crate::manifest::ManifestStore::load_latest(&self.page_store)? {
            Some(loaded) => loaded,
            None => {
                let mut report = crate::verify::VerifyReport {
                    high_water: self.page_store.high_water(),
                    ..crate::verify::VerifyReport::default()
                };
                report
                    .issues
                    .push("no valid manifest slot could be decoded".into());
                return Ok(report);
            }
        };
        crate::verify::verify_page_store(&self.page_store, &loaded, options)
    }
}
