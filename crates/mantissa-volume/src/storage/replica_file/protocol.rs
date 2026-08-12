//! Cap'n Proto encoding for fixed replica files and their recovery records.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::Cursor;
use std::os::unix::fs::FileExt;

use capnp::message::{Builder, ReaderOptions};
use mantissa_protocol::volumes::{stored_volume_changed_regions, stored_volume_file_header};

use crate::protocol::{read_descriptor, write_descriptor};
use crate::storage_format::{
    MAX_CHANGED_REGION_RECORD_BYTES_PER_REGION, MAX_CHANGED_REGION_RECORD_FIXED_BYTES,
};
use crate::{FenceEpoch, VolumeDescriptor};

use super::{FILE_HEADER_SLOT_BYTES, ReplicaBlockChange, ReplicaFileError};

const FILE_FORMAT_VERSION: u16 = 1;
const CHANGED_REGION_FORMAT_VERSION: u16 = 1;
const LENGTH_BYTES: usize = size_of::<u32>();
const DIGEST_BYTES: usize = 32;
const MAX_NESTING_LEVELS: i32 = 16;

/// Durable fields recovered from either checked header space.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FileHeader {
    pub descriptor: VolumeDescriptor,
    pub data_fence: FenceEpoch,
    pub flush_number: u64,
    pub through_write_number: u64,
    pub changed_region_generation: u64,
}

/// Complete valid prefix recovered from the changed-region file.
pub(super) struct ChangedRegionState {
    pub generation: u64,
    pub sequence: u64,
    pub digest: [u8; DIGEST_BYTES],
    pub regions: BTreeSet<u64>,
    pub complete_bytes: u64,
}

/// Framed changed-region record and the digest required by its successor.
pub(super) struct EncodedChangedRegions {
    pub bytes: Vec<u8>,
    pub digest: [u8; DIGEST_BYTES],
}

/// Calculates the exact digest shared by every copy and request retry.
pub(super) fn write_digest(
    descriptor: &VolumeDescriptor,
    data_fence: FenceEpoch,
    write_number: u64,
    changes: &[ReplicaBlockChange],
) -> [u8; DIGEST_BYTES] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mantissa replica write v1\0");
    update_descriptor(&mut hasher, descriptor);
    hasher.update(&data_fence.get().to_le_bytes());
    hasher.update(&write_number.to_le_bytes());
    hasher.update(&(changes.len() as u64).to_le_bytes());
    for change in changes {
        hasher.update(&change.block().to_le_bytes());
        match change {
            ReplicaBlockChange::Write { data, .. } => {
                hasher.update(&[1]);
                hasher.update(&(data.len() as u64).to_le_bytes());
                // Hash the complete block in one call so BLAKE3 can use its
                // parallel implementation. Interleaving each block with the
                // small typed fields forces ARM onto the serial compression
                // path and made the ublk path CPU-bound.
                hasher.update(blake3::hash(data).as_bytes());
            }
            ReplicaBlockChange::Zero { .. } => {
                hasher.update(&[2]);
            }
            ReplicaBlockChange::Discard { .. } => {
                hasher.update(&[3]);
            }
        }
    }
    *hasher.finalize().as_bytes()
}

/// Encodes one header and pads it to its fixed on-disk space.
pub(super) fn encode_header_slot(header: &FileHeader) -> Result<Vec<u8>, ReplicaFileError> {
    let digest = header_digest(header);
    let mut message = Builder::new_default();
    {
        let mut root = message.init_root::<stored_volume_file_header::Builder<'_>>();
        root.set_format_version(FILE_FORMAT_VERSION);
        write_descriptor(root.reborrow().init_descriptor(), &header.descriptor);
        root.set_data_fence(header.data_fence.get());
        root.set_flush_number(header.flush_number);
        root.set_through_write_number(header.through_write_number);
        root.set_changed_region_generation(header.changed_region_generation);
        root.set_digest(&digest);
    }
    let message_bytes = capnp::serialize::write_message_to_words(&message);
    let slot_bytes = usize::try_from(FILE_HEADER_SLOT_BYTES)
        .map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    let framed_bytes = LENGTH_BYTES
        .checked_add(message_bytes.len())
        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    if framed_bytes > slot_bytes {
        return Err(ReplicaFileError::InvalidHeaderRecordLength(
            message_bytes.len() as u64,
        ));
    }
    let message_length = u32::try_from(message_bytes.len())
        .map_err(|_| ReplicaFileError::InvalidHeaderRecordLength(message_bytes.len() as u64))?;
    let mut slot = vec![0_u8; slot_bytes];
    slot[..LENGTH_BYTES].copy_from_slice(&message_length.to_le_bytes());
    slot[LENGTH_BYTES..framed_bytes].copy_from_slice(&message_bytes);
    Ok(slot)
}

/// Decodes and checks one fixed header space.
pub(super) fn decode_header_slot(
    slot: &[u8],
    expected_descriptor: &VolumeDescriptor,
) -> Result<Option<FileHeader>, ReplicaFileError> {
    if slot.iter().all(|byte| *byte == 0) {
        return Ok(None);
    }
    let slot_bytes = usize::try_from(FILE_HEADER_SLOT_BYTES)
        .map_err(|_| ReplicaFileError::FileOffsetOverflow)?;
    if slot.len() != slot_bytes {
        return Err(ReplicaFileError::InvalidHeaderRecordLength(
            slot.len() as u64
        ));
    }
    let message_bytes = u32::from_le_bytes(
        slot[..LENGTH_BYTES]
            .try_into()
            .map_err(|_| ReplicaFileError::InvalidHeaderRecordLength(slot.len() as u64))?,
    ) as usize;
    let message_end = LENGTH_BYTES.checked_add(message_bytes).ok_or(
        ReplicaFileError::InvalidHeaderRecordLength(message_bytes as u64),
    )?;
    if message_bytes == 0 || message_end > slot.len() || !message_bytes.is_multiple_of(8) {
        return Err(ReplicaFileError::InvalidHeaderRecordLength(
            message_bytes as u64,
        ));
    }
    if slot[message_end..].iter().any(|byte| *byte != 0) {
        return Err(ReplicaFileError::HeaderTrailingBytes);
    }
    let message = read_message(&slot[LENGTH_BYTES..message_end], message_bytes)?;
    let root = message.get_root::<stored_volume_file_header::Reader<'_>>()?;
    if root.get_format_version() != FILE_FORMAT_VERSION {
        return Err(ReplicaFileError::UnsupportedHeaderFormat(
            root.get_format_version(),
        ));
    }
    let descriptor = read_descriptor(root.get_descriptor()?)?;
    if !descriptor.has_same_storage_identity(expected_descriptor)
        || descriptor.capacity() > expected_descriptor.capacity()
    {
        return Err(ReplicaFileError::WrongDescriptor);
    }
    let data_fence =
        FenceEpoch::new(root.get_data_fence()).map_err(crate::protocol::ProtocolError::from)?;
    let changed_region_generation = root.get_changed_region_generation();
    if changed_region_generation == 0 {
        return Err(ReplicaFileError::WrongChangedRegionIdentity);
    }
    let header = FileHeader {
        descriptor,
        data_fence,
        flush_number: root.get_flush_number(),
        through_write_number: root.get_through_write_number(),
        changed_region_generation,
    };
    let saved_digest = read_fixed(root.get_digest()?, "file header digest")?;
    if saved_digest != header_digest(&header) {
        return Err(ReplicaFileError::WrongHeaderDigest);
    }
    Ok(Some(header))
}

/// Encodes one checked list of regions that became dirty before data changed.
pub(super) fn encode_changed_regions(
    descriptor: &VolumeDescriptor,
    generation: u64,
    sequence: u64,
    region_bytes: u64,
    regions: &[u64],
    previous_digest: [u8; DIGEST_BYTES],
) -> Result<EncodedChangedRegions, ReplicaFileError> {
    let digest = changed_regions_digest(
        descriptor,
        generation,
        sequence,
        region_bytes,
        regions,
        previous_digest,
    );
    let mut message = Builder::new_default();
    {
        let mut root = message.init_root::<stored_volume_changed_regions::Builder<'_>>();
        root.set_format_version(CHANGED_REGION_FORMAT_VERSION);
        root.set_volume_id(descriptor.volume_id().as_bytes());
        root.set_data_generation(descriptor.generation().get());
        root.set_changed_region_generation(generation);
        root.set_sequence(sequence);
        root.set_region_bytes(region_bytes);
        let count = u32::try_from(regions.len()).map_err(|_| {
            ReplicaFileError::InvalidChangedRegionRecordLength(regions.len() as u64)
        })?;
        let mut saved_regions = root.reborrow().init_regions(count);
        for (index, region) in regions.iter().enumerate() {
            saved_regions.set(index as u32, *region);
        }
        root.set_previous_digest(&previous_digest);
        root.set_digest(&digest);
    }
    let message_bytes = capnp::serialize::write_message_to_words(&message);
    let message_length = u32::try_from(message_bytes.len()).map_err(|_| {
        ReplicaFileError::InvalidChangedRegionRecordLength(message_bytes.len() as u64)
    })?;
    let mut bytes = Vec::with_capacity(LENGTH_BYTES.saturating_add(message_bytes.len()));
    bytes.extend_from_slice(&message_length.to_le_bytes());
    bytes.extend_from_slice(&message_bytes);
    let maximum_bytes = u64::try_from(regions.len())
        .ok()
        .and_then(|count| count.checked_mul(MAX_CHANGED_REGION_RECORD_BYTES_PER_REGION))
        .and_then(|bytes| bytes.checked_add(MAX_CHANGED_REGION_RECORD_FIXED_BYTES))
        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(ReplicaFileError::InvalidChangedRegionRecordLength(
            bytes.len() as u64,
        ));
    }
    Ok(EncodedChangedRegions { bytes, digest })
}

/// Reads every complete record and leaves an incomplete final write for repair.
pub(super) fn decode_changed_regions(
    file: &mut File,
    descriptor: &VolumeDescriptor,
    generation: u64,
    region_bytes: u64,
    max_changes_per_write: usize,
) -> Result<ChangedRegionState, ReplicaFileError> {
    let file_bytes = file
        .metadata()
        .map_err(|source| ReplicaFileError::Io {
            operation: "read changed-region file length",
            source,
        })?
        .len();
    let maximum_message_bytes = max_changes_per_write
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    let region_count = descriptor.capacity().bytes().div_ceil(region_bytes);
    let mut offset = 0_u64;
    let mut sequence = 0_u64;
    let mut digest = [0_u8; DIGEST_BYTES];
    let mut regions = BTreeSet::new();

    while offset < file_bytes {
        let remaining = file_bytes - offset;
        if remaining < LENGTH_BYTES as u64 {
            break;
        }
        let mut length = [0_u8; LENGTH_BYTES];
        read_exact_at(
            file,
            &mut length,
            offset,
            "read changed-region record length",
        )?;
        let message_bytes = u32::from_le_bytes(length) as usize;
        if message_bytes == 0
            || message_bytes > maximum_message_bytes
            || !message_bytes.is_multiple_of(8)
        {
            return Err(ReplicaFileError::InvalidChangedRegionRecordLength(
                message_bytes as u64,
            ));
        }
        let framed_bytes = LENGTH_BYTES
            .checked_add(message_bytes)
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
        if remaining < framed_bytes as u64 {
            break;
        }
        let mut saved = vec![0_u8; message_bytes];
        read_exact_at(
            file,
            &mut saved,
            offset + LENGTH_BYTES as u64,
            "read changed-region record",
        )?;
        let message = read_message(&saved, maximum_message_bytes)?;
        let root = message.get_root::<stored_volume_changed_regions::Reader<'_>>()?;
        if root.get_format_version() != CHANGED_REGION_FORMAT_VERSION {
            return Err(ReplicaFileError::UnsupportedChangedRegionFormat(
                root.get_format_version(),
            ));
        }
        if root.get_volume_id()? != descriptor.volume_id().as_bytes()
            || root.get_data_generation() != descriptor.generation().get()
            || root.get_changed_region_generation() != generation
            || root.get_region_bytes() != region_bytes
        {
            return Err(ReplicaFileError::WrongChangedRegionIdentity);
        }
        let expected_sequence = sequence
            .checked_add(1)
            .ok_or(ReplicaFileError::ChangedRegionSequenceExhausted)?;
        if root.get_sequence() != expected_sequence {
            return Err(ReplicaFileError::UnexpectedChangedRegionSequence {
                actual: root.get_sequence(),
                expected: expected_sequence,
            });
        }
        let saved_previous = read_fixed(root.get_previous_digest()?, "previous region digest")?;
        if saved_previous != digest {
            return Err(ReplicaFileError::WrongChangedRegionDigest);
        }
        let saved_regions = root.get_regions()?;
        if saved_regions.is_empty() || saved_regions.len() as usize > max_changes_per_write {
            return Err(ReplicaFileError::UnorderedChangedRegions);
        }
        let mut record_regions = Vec::with_capacity(saved_regions.len() as usize);
        let mut previous = None;
        for region in saved_regions.iter() {
            if previous.is_some_and(|value| region <= value) {
                return Err(ReplicaFileError::UnorderedChangedRegions);
            }
            if region >= region_count {
                return Err(ReplicaFileError::ChangedRegionOutsideVolume {
                    region,
                    region_count,
                });
            }
            previous = Some(region);
            record_regions.push(region);
        }
        let expected_digest = changed_regions_digest(
            descriptor,
            generation,
            expected_sequence,
            region_bytes,
            &record_regions,
            digest,
        );
        let saved_digest = read_fixed(root.get_digest()?, "changed-region digest")?;
        if saved_digest != expected_digest {
            return Err(ReplicaFileError::WrongChangedRegionDigest);
        }
        regions.extend(record_regions);
        sequence = expected_sequence;
        digest = expected_digest;
        offset = offset
            .checked_add(framed_bytes as u64)
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    }

    Ok(ChangedRegionState {
        generation,
        sequence,
        digest,
        regions,
        complete_bytes: offset,
    })
}

/// Calculates the header digest without depending on Cap'n Proto layout.
fn header_digest(header: &FileHeader) -> [u8; DIGEST_BYTES] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mantissa replica file header v1\0");
    update_descriptor(&mut hasher, &header.descriptor);
    hasher.update(&header.data_fence.get().to_le_bytes());
    hasher.update(&header.flush_number.to_le_bytes());
    hasher.update(&header.through_write_number.to_le_bytes());
    hasher.update(&header.changed_region_generation.to_le_bytes());
    *hasher.finalize().as_bytes()
}

/// Calculates one changed-region record digest including its previous link.
fn changed_regions_digest(
    descriptor: &VolumeDescriptor,
    generation: u64,
    sequence: u64,
    region_bytes: u64,
    regions: &[u64],
    previous_digest: [u8; DIGEST_BYTES],
) -> [u8; DIGEST_BYTES] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mantissa changed regions v1\0");
    hasher.update(descriptor.volume_id().as_bytes());
    hasher.update(&descriptor.generation().get().to_le_bytes());
    hasher.update(&generation.to_le_bytes());
    hasher.update(&sequence.to_le_bytes());
    hasher.update(&region_bytes.to_le_bytes());
    hasher.update(&(regions.len() as u64).to_le_bytes());
    for region in regions {
        hasher.update(&region.to_le_bytes());
    }
    hasher.update(&previous_digest);
    *hasher.finalize().as_bytes()
}

/// Adds every descriptor field to a stable digest.
fn update_descriptor(hasher: &mut blake3::Hasher, descriptor: &VolumeDescriptor) {
    hasher.update(descriptor.volume_id().as_bytes());
    hasher.update(&descriptor.generation().get().to_le_bytes());
    hasher.update(&descriptor.capacity().bytes().to_le_bytes());
    let sizes = descriptor.block_sizes();
    hasher.update(&sizes.logical_sector().bytes().to_le_bytes());
    hasher.update(&sizes.physical_block().bytes().to_le_bytes());
    hasher.update(&sizes.minimum_io().bytes().to_le_bytes());
    hasher.update(&sizes.data_block().bytes().to_le_bytes());
}

/// Opens one bounded Cap'n Proto message and rejects bytes after it.
fn read_message(
    bytes: &[u8],
    maximum_bytes: usize,
) -> Result<capnp::message::Reader<capnp::serialize::OwnedSegments>, ReplicaFileError> {
    if bytes.len() > maximum_bytes {
        return Err(ReplicaFileError::InvalidChangedRegionRecordLength(
            bytes.len() as u64,
        ));
    }
    let mut options = ReaderOptions::new();
    options
        .traversal_limit_in_words(Some(maximum_bytes.div_ceil(8)))
        .nesting_limit(MAX_NESTING_LEVELS);
    let mut cursor = Cursor::new(bytes);
    let message = capnp::serialize::read_message(&mut cursor, options)?;
    if cursor.position() != bytes.len() as u64 {
        return Err(ReplicaFileError::CapnpTrailingBytes);
    }
    Ok(message)
}

/// Reads one exact-width byte field.
fn read_fixed<const N: usize>(
    bytes: &[u8],
    field: &'static str,
) -> Result<[u8; N], ReplicaFileError> {
    bytes
        .try_into()
        .map_err(|_| ReplicaFileError::InvalidFieldLength {
            field,
            actual: bytes.len(),
            expected: N,
        })
}

/// Completes a positioned metadata read after checking its file range.
fn read_exact_at(
    file: &File,
    mut output: &mut [u8],
    mut offset: u64,
    operation: &'static str,
) -> Result<(), ReplicaFileError> {
    while !output.is_empty() {
        let read = file
            .read_at(output, offset)
            .map_err(|source| ReplicaFileError::Io { operation, source })?;
        if read == 0 {
            return Err(ReplicaFileError::Io {
                operation,
                source: std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "positioned metadata read ended early",
                ),
            });
        }
        output = &mut output[read..];
        offset = offset
            .checked_add(read as u64)
            .ok_or(ReplicaFileError::FileOffsetOverflow)?;
    }
    Ok(())
}
