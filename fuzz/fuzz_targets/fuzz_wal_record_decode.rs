#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    onyx_metadb::fuzz::wal_record_decode(data);
});
