use super::*;

mod apply;
mod commit;
mod query;
mod snapshot;
mod storage;

impl MetaMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}
