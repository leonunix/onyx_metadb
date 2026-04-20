#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    onyx_metadb::fuzz::bloom_bits_decode(data);
});
