use mantissa_volume::{
    BlockSizeError, BlockSizeSetting, DescriptorError, LogicalBlockNumber, VolumeBlockSizes,
    VolumeDescriptor, VolumeGeneration, VolumeId,
};
use uuid::Uuid;

const KIB: u64 = 1024;
const TIB: u64 = KIB * KIB * KIB * KIB;
const BLOCK_BYTES: u32 = 4 * 1024;

/// Creates one deterministic non-nil identity for descriptor tests.
fn volume_id() -> VolumeId {
    VolumeId::new(Uuid::from_u128(0x018f_89ad_6bc8_7b3d_a8ef_50b1_3cda_14c2))
        .expect("the fixed UUID must be non-nil")
}

/// Creates one valid non-zero generation for tests.
fn generation(value: u64) -> VolumeGeneration {
    VolumeGeneration::new(value).expect("the test generation must be non-zero")
}

/// Creates a descriptor using the first supported set of block sizes.
fn descriptor(capacity_bytes: u64, generation_value: u64) -> VolumeDescriptor {
    VolumeDescriptor::new(
        volume_id(),
        generation(generation_value),
        capacity_bytes,
        VolumeBlockSizes::supported(),
    )
    .expect("the test descriptor must be valid")
}

#[test]
fn every_block_size_is_validated_independently() {
    let cases = [
        (
            BlockSizeSetting::LogicalSector,
            [8 * 1024, BLOCK_BYTES, BLOCK_BYTES, BLOCK_BYTES, BLOCK_BYTES],
        ),
        (
            BlockSizeSetting::PhysicalBlock,
            [BLOCK_BYTES, 8 * 1024, BLOCK_BYTES, BLOCK_BYTES, BLOCK_BYTES],
        ),
        (
            BlockSizeSetting::MinimumIo,
            [BLOCK_BYTES, BLOCK_BYTES, 8 * 1024, BLOCK_BYTES, BLOCK_BYTES],
        ),
        (
            BlockSizeSetting::DataBlock,
            [BLOCK_BYTES, BLOCK_BYTES, BLOCK_BYTES, 8 * 1024, BLOCK_BYTES],
        ),
    ];

    for (expected_block_size, values) in cases {
        let error = VolumeBlockSizes::new(values[0], values[1], values[2], values[3])
            .expect_err("an unsupported block size must fail");
        assert_eq!(
            error,
            BlockSizeError::Unsupported {
                setting: expected_block_size,
                actual_bytes: 8 * 1024,
                required_bytes: BLOCK_BYTES,
            }
        );
    }
}

#[test]
fn descriptor_has_no_arbitrary_eight_or_sixty_four_tib_ceiling() {
    let capacities = [9 * TIB, 65 * TIB, 128 * TIB, u64::MAX - 4095];

    for capacity_bytes in capacities {
        let descriptor = descriptor(capacity_bytes, 1);
        assert_eq!(descriptor.capacity().bytes(), capacity_bytes);

        let last_block = descriptor.logical_block_count() - 1;
        let last_offset = descriptor
            .byte_offset(LogicalBlockNumber::new(last_block))
            .expect("the final aligned logical block must be addressable");
        assert_eq!(last_offset.bytes(), capacity_bytes - u64::from(BLOCK_BYTES));
    }
}

#[test]
fn descriptor_rejects_invalid_capacity_and_checked_address_overflow() {
    let zero_capacity =
        VolumeDescriptor::new(volume_id(), generation(1), 0, VolumeBlockSizes::supported())
            .expect_err("zero capacity must fail");
    assert_eq!(zero_capacity, DescriptorError::ZeroCapacity);

    let unaligned_capacity = VolumeDescriptor::new(
        volume_id(),
        generation(1),
        u64::from(BLOCK_BYTES) + 1,
        VolumeBlockSizes::supported(),
    )
    .expect_err("an unaligned capacity must fail");
    assert_eq!(
        unaligned_capacity,
        DescriptorError::MisalignedCapacity {
            capacity_bytes: u64::from(BLOCK_BYTES) + 1,
            setting: BlockSizeSetting::LogicalSector,
            alignment_bytes: BLOCK_BYTES,
        }
    );

    let descriptor = descriptor(u64::from(BLOCK_BYTES), 1);
    let overflow = descriptor
        .byte_offset(LogicalBlockNumber::new(u64::MAX))
        .expect_err("logical-block multiplication must be checked");
    assert_eq!(
        overflow,
        DescriptorError::ByteOffsetOverflow {
            block_number: u64::MAX,
            logical_sector_bytes: u64::from(BLOCK_BYTES),
        }
    );

    let out_of_bounds = descriptor
        .byte_offset(LogicalBlockNumber::new(1))
        .expect_err("the first block after the volume must fail");
    assert_eq!(
        out_of_bounds,
        DescriptorError::BlockOutOfBounds {
            block_number: 1,
            logical_block_count: 1,
        }
    );
}
