use std::io::{self, Read, Write};

use super::{MetadataStore, Snapshot};

pub struct BincodeStore;

impl MetadataStore for BincodeStore {
    fn open<R: Read>(&self, reader: R) -> io::Result<Snapshot> {
        bincode::deserialize_from(reader).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn save<W: Write>(&self, snapshot: &Snapshot, writer: W) -> io::Result<()> {
        bincode::serialize_into(writer, snapshot)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}
