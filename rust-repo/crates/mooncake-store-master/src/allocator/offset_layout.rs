use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetAllocatorReport {
    pub allocated_bytes: u64,
    pub allocation_count: u64,
    pub capacity: u64,
    pub total_free_space: u64,
    pub largest_free_region: u64,
}

pub(super) fn preferred_segment_names(config: &ReplicateConfig) -> Vec<&str> {
    if !config.preferred_segment.is_empty() {
        return vec![config.preferred_segment.as_str()];
    }
    config
        .preferred_segments
        .iter()
        .filter(|name| !name.is_empty())
        .map(String::as_str)
        .collect()
}

impl SegmentState {
    pub(super) fn offset_allocator_report(&self) -> Option<OffsetAllocatorReport> {
        let SegmentLayout::Offset(offset) = &self.layout else {
            return None;
        };
        let allocated_bytes = offset
            .allocations
            .values()
            .try_fold(0_u64, |total, &size| total.checked_add(size))?;
        let total_free_space = offset
            .free_ranges
            .iter()
            .try_fold(0_u64, |total, &(_, size)| total.checked_add(size))?;
        let largest_free_region = offset
            .free_ranges
            .iter()
            .map(|&(_, size)| size)
            .max()
            .unwrap_or(0);

        Some(OffsetAllocatorReport {
            allocated_bytes,
            allocation_count: u64::try_from(offset.allocations.len()).ok()?,
            capacity: self.segment.size,
            total_free_space,
            largest_free_region,
        })
    }

    /// Attempt to allocate `size` bytes from this segment.
    /// 尝试从此 segment 分配 `size` 字节。
    ///
    /// Returns Some((offset, accounted_size)) on success.
    /// Note: accounted_size may be larger than size due to size class rounding (Cachelib).
    /// 成功时返回 Some((偏移量, 计入大小))。
    /// 注意：由于 size class 取整（Cachelib），计入大小可能大于请求大小。
    pub(super) fn allocate(&mut self, size: u64) -> Option<(u64, u64)> {
        match &mut self.layout {
            SegmentLayout::Offset(offset) => {
                let active_nodes = u64::try_from(offset.allocations.len())
                    .ok()?
                    .checked_add(u64::try_from(offset.free_ranges.len()).ok()?)?;
                if offset
                    .max_allocation_nodes
                    .is_some_and(|maximum| active_nodes >= maximum)
                {
                    return None;
                }
                let start = reserve_range(&mut offset.free_ranges, size)?;
                if offset.allocations.insert(start, size).is_some() {
                    insert_free_range(&mut offset.free_ranges, start, size);
                    return None;
                }
                Some((start, size))
            }
            SegmentLayout::Cachelib(cachelib) => allocate_cachelib(cachelib, size),
        }
    }

    pub(super) fn validate_release(&self, replica: &ReplicaDescriptor) -> Result<u64, String> {
        if replica.size == 0 {
            return Err("cannot release a zero-sized allocation".to_string());
        }
        match &self.layout {
            SegmentLayout::Offset(offset) => match offset.allocations.get(&replica.offset) {
                Some(size) if *size == replica.size => Ok(*size),
                Some(size) => Err(format!(
                    "offset allocation size mismatch at {}: descriptor={}, allocator={size}",
                    replica.offset, replica.size
                )),
                None => Err(format!(
                    "offset allocation is not live at {}",
                    replica.offset
                )),
            },
            SegmentLayout::Cachelib(cachelib) => {
                let allocation = cachelib.allocations.get(&replica.offset).ok_or_else(|| {
                    format!("cachelib allocation is not live at {}", replica.offset)
                })?;
                if allocation.requested_size != replica.size {
                    return Err(format!(
                        "cachelib allocation size mismatch at {}: descriptor={}, allocator={}",
                        replica.offset, replica.size, allocation.requested_size
                    ));
                }
                Ok(allocation.class_size)
            }
        }
    }

    /// Release the space occupied by a replica.
    /// 释放副本占用的空间。
    pub(super) fn release(&mut self, replica: &ReplicaDescriptor) -> Option<u64> {
        self.validate_release(replica).ok()?;
        match &mut self.layout {
            SegmentLayout::Offset(offset) => {
                let released_size = offset.allocations.remove(&replica.offset)?;
                insert_free_range(&mut offset.free_ranges, replica.offset, released_size);
                Some(released_size)
            }
            SegmentLayout::Cachelib(cachelib) => release_cachelib(cachelib, replica.offset),
        }
    }
}

/// Compute the free ratio of a segment: free / total.
/// 计算 segment 的空闲率：free / total。
/// Used by FreeRatioFirst strategy for segment sorting.
/// 由 FreeRatioFirst 策略用于 segment 排序。
pub(super) fn free_ratio(size: u64, used: u64) -> f64 {
    if size == 0 {
        return 0.0;
    }
    size.saturating_sub(used) as f64 / size as f64
}

/// Reserve `size` bytes from the free ranges list, returning the starting offset.
/// 从空闲区间列表中预留 size 字节，返回起始偏移量。
///
/// Uses the first sufficiently large range; removes the range if exact fit,
/// otherwise shrinks it from the left.
/// 优先使用首个足够大的区间；若区间刚好吃完则移除，否则从左侧收缩。
fn reserve_range(free_ranges: &mut Vec<(u64, u64)>, size: u64) -> Option<u64> {
    let idx = free_ranges.iter().position(|(_, len)| *len >= size)?;
    let (offset, len) = free_ranges[idx];
    if len == size {
        free_ranges.remove(idx);
    } else {
        free_ranges[idx] = (offset + size, len - size);
    }
    Some(offset)
}

/// Insert a free range into the list, automatically merging adjacent/overlapping ranges.
/// 向空闲区间列表插入一个新区间，并自动合并相邻/重叠区间。
///
/// Algorithm: sort by start offset, then one-pass merge: if current range
/// overlaps or touches the previous, extend the previous range.
/// 先排序后遍历合并：若新区间与前一区间重叠或相邻，则扩展前一区间。
fn insert_free_range(free_ranges: &mut Vec<(u64, u64)>, offset: u64, len: u64) {
    free_ranges.push((offset, len));
    free_ranges.sort_by_key(|(start, _)| *start);

    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(free_ranges.len());
    for (start, size) in free_ranges.drain(..) {
        if let Some((prev_start, prev_size)) = merged.last_mut() {
            let prev_end = *prev_start + *prev_size;
            let current_end = start + size;
            if start <= prev_end {
                *prev_size = (*prev_size).max(current_end.saturating_sub(*prev_start));
                continue;
            }
        }
        merged.push((start, size));
    }
    *free_ranges = merged;
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;
    const CPP_NONZERO_BIN_SIZES: [u64; 239] = [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        18,
        20,
        22,
        24,
        26,
        28,
        30,
        32,
        36,
        40,
        44,
        48,
        52,
        56,
        60,
        64,
        72,
        80,
        88,
        96,
        104,
        112,
        120,
        128,
        144,
        160,
        176,
        192,
        208,
        224,
        240,
        256,
        288,
        320,
        352,
        384,
        416,
        448,
        480,
        512,
        576,
        640,
        704,
        768,
        832,
        896,
        960,
        1_024,
        1_152,
        1_280,
        1_408,
        1_536,
        1_664,
        1_792,
        1_920,
        2_048,
        2_304,
        2_560,
        2_816,
        3_072,
        3_328,
        3_584,
        3_840,
        4_096,
        4_608,
        5_120,
        5_632,
        6_144,
        6_656,
        7_168,
        7_680,
        8_192,
        9_216,
        10_240,
        11_264,
        12_288,
        13_312,
        14_336,
        15_360,
        16_384,
        18_432,
        20_480,
        22_528,
        24_576,
        26_624,
        28_672,
        30_720,
        32_768,
        36_864,
        40_960,
        45_056,
        49_152,
        53_248,
        57_344,
        61_440,
        65_536,
        73_728,
        81_920,
        90_112,
        98_304,
        106_496,
        114_688,
        122_880,
        131_072,
        147_456,
        163_840,
        180_224,
        196_608,
        212_992,
        229_376,
        245_760,
        262_144,
        294_912,
        327_680,
        360_448,
        393_216,
        425_984,
        458_752,
        491_520,
        524_288,
        589_824,
        655_360,
        720_896,
        786_432,
        851_968,
        917_504,
        983_040,
        1_048_576,
        1_179_648,
        1_310_720,
        1_441_792,
        1_572_864,
        1_703_936,
        1_835_008,
        1_966_080,
        2_097_152,
        2_359_296,
        2_621_440,
        2_883_584,
        3_145_728,
        3_407_872,
        3_670_016,
        3_932_160,
        4_194_304,
        4_718_592,
        5_242_880,
        5_767_168,
        6_291_456,
        6_815_744,
        7_340_032,
        7_864_320,
        8_388_608,
        9_437_184,
        10_485_760,
        11_534_336,
        12_582_912,
        13_631_488,
        14_680_064,
        15_728_640,
        16_777_216,
        18_874_368,
        20_971_520,
        23_068_672,
        25_165_824,
        27_262_976,
        29_360_128,
        31_457_280,
        33_554_432,
        37_748_736,
        41_943_040,
        46_137_344,
        50_331_648,
        54_525_952,
        58_720_256,
        62_914_560,
        67_108_864,
        75_497_472,
        83_886_080,
        92_274_688,
        100_663_296,
        109_051_904,
        117_440_512,
        125_829_120,
        134_217_728,
        150_994_944,
        167_772_160,
        184_549_376,
        201_326_592,
        218_103_808,
        234_881_024,
        251_658_240,
        268_435_456,
        301_989_888,
        335_544_320,
        369_098_752,
        402_653_184,
        436_207_616,
        469_762_048,
        503_316_480,
        536_870_912,
        603_979_776,
        671_088_640,
        738_197_504,
        805_306_368,
        872_415_232,
        939_524_096,
        1_006_632_960,
        1_073_741_824,
        1_207_959_552,
        1_342_177_280,
        1_476_395_008,
        1_610_612_736,
        1_744_830_464,
        1_879_048_192,
        2_013_265_920,
        2_147_483_648,
        2_415_919_104,
        2_684_354_560,
        2_952_790_016,
        3_221_225_472,
        3_489_660_928,
        3_758_096_384,
        4_026_531_840,
    ];

    fn offset_state(capacity: u64) -> SegmentState {
        SegmentState {
            segment: Segment {
                id: Uuid::new_v4(),
                name: "offset-parity".to_string(),
                base: 16 * 1024,
                size: capacity,
                te_endpoint: "127.0.0.1:12345".to_string(),
                protocol: "tcp".to_string(),
                host_id: "offset-parity-host".to_string(),
            },
            used: 0,
            layout: SegmentLayout::Offset(OffsetSegmentState {
                free_ranges: vec![(0, capacity)],
                allocations: HashMap::new(),
                max_allocation_nodes: None,
            }),
            client_id: Uuid::new_v4(),
            runtime_bound: true,
        }
    }

    fn offset_allocator_facade(capacity: u64) -> (SegmentAllocator, Uuid) {
        let mut allocator = SegmentAllocator::new();
        let segment_id = Uuid::new_v4();
        allocator.add_segment(
            Segment {
                id: segment_id,
                name: "offset-report-parity".to_string(),
                base: 16 * 1024,
                size: capacity,
                te_endpoint: "127.0.0.1:12345".to_string(),
                protocol: "tcp".to_string(),
                host_id: "offset-report-host".to_string(),
            },
            0,
            Uuid::new_v4(),
        );
        (allocator, segment_id)
    }

    fn limited_offset_allocator_facade(
        capacity: u64,
        max_allocations: u64,
    ) -> (SegmentAllocator, Uuid) {
        let mut allocator = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(max_allocations))
            .expect("valid Offset node budget");
        let segment_id = Uuid::new_v4();
        allocator
            .try_add_segment(
                Segment {
                    id: segment_id,
                    name: "offset-limit-parity".to_string(),
                    base: 16 * 1024,
                    size: capacity,
                    te_endpoint: "127.0.0.1:12345".to_string(),
                    protocol: "tcp".to_string(),
                    host_id: "offset-limit-host".to_string(),
                },
                0,
                Uuid::new_v4(),
            )
            .expect("limited Offset segment");
        (allocator, segment_id)
    }

    fn allocate_facade_exact(
        allocator: &mut SegmentAllocator,
        segment_id: Uuid,
        capacity: u64,
        size: u64,
    ) -> ReplicaDescriptor {
        let replica = allocator
            .allocate_from_segment_id(segment_id, size)
            .expect("offset allocation");
        assert_eq!(replica.segment_id, segment_id);
        assert_eq!(replica.size, size);
        assert!(replica.offset.checked_add(size).unwrap() <= capacity);
        replica
    }

    fn release_facade_exact(allocator: &mut SegmentAllocator, replica: ReplicaDescriptor) {
        allocator.release(&[replica]).expect("offset release");
    }

    #[test]
    fn offset_report_rejects_unknown_cachelib_and_cxl_aliases() {
        let unknown = SegmentAllocator::new();
        assert_eq!(unknown.offset_allocator_report(&Uuid::new_v4()), None);

        let mut cachelib =
            SegmentAllocator::new().with_memory_allocator(MemoryAllocatorKind::CachelibLike);
        let cachelib_id = Uuid::new_v4();
        cachelib.add_segment(
            Segment {
                id: cachelib_id,
                name: "cachelib-report-negative".to_string(),
                base: 0,
                size: 4 * MIB,
                te_endpoint: "127.0.0.1:12345".to_string(),
                protocol: "tcp".to_string(),
                host_id: "cachelib-report-host".to_string(),
            },
            0,
            Uuid::new_v4(),
        );
        assert_eq!(cachelib.offset_allocator_report(&cachelib_id), None);

        let mut cxl = SegmentAllocator::new().with_cxl_capacity(4 * MIB);
        let cxl_id = Uuid::new_v4();
        cxl.add_segment(
            Segment {
                id: cxl_id,
                name: "cxl-report-negative".to_string(),
                base: 0,
                size: 4 * MIB,
                te_endpoint: "127.0.0.1:12345".to_string(),
                protocol: "cxl".to_string(),
                host_id: "cxl-report-host".to_string(),
            },
            0,
            Uuid::new_v4(),
        );
        assert_eq!(cxl.offset_allocator_report(&cxl_id), None);
    }

    fn allocate_exact(state: &mut SegmentState, capacity: u64, size: u64) -> (u64, u64) {
        let allocation = state.allocate(size).expect("offset allocation");
        assert_eq!(allocation.1, size);
        assert!(allocation.0.checked_add(size).unwrap() <= capacity);
        allocation
    }

    fn release_exact(state: &mut SegmentState, allocation: (u64, u64)) {
        let replica = ReplicaDescriptor {
            segment_id: state.segment.id,
            segment_name: state.segment.name.clone(),
            offset: allocation.0,
            size: allocation.1,
            status: ReplicaStatus::Complete,
            replica_type: ReplicaType::Memory,
            holder_client_id: Some(state.client_id),
            local_disk_storage_id: None,
            local_disk_generation_id: None,
            refcnt: 0,
            handle_valid: true,
            base_addr: state.segment.base + allocation.0,
            protocol: state.segment.protocol.clone(),
        };
        assert_eq!(state.release(&replica), Some(allocation.1));
    }

    fn assert_live_ranges(ranges: &[(u64, u64)], capacity: u64) {
        for &(offset, size) in ranges {
            assert!(size > 0);
            assert!(offset.checked_add(size).unwrap() <= capacity);
        }
        for (index, &(left_offset, left_size)) in ranges.iter().enumerate() {
            let left_end = left_offset + left_size;
            for &(right_offset, right_size) in &ranges[index + 1..] {
                let right_end = right_offset + right_size;
                assert!(left_end <= right_offset || right_end <= left_offset);
            }
        }
    }

    struct DeterministicRng(u64);

    impl DeterministicRng {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 ^ (self.0 >> 29)
        }

        fn one_through(&mut self, maximum: u64) -> u64 {
            1 + self.next() % maximum
        }

        fn inclusive(&mut self, minimum: u64, maximum: u64) -> u64 {
            minimum + self.next() % (maximum - minimum + 1)
        }

        fn index(&mut self, length: usize) -> usize {
            self.next() as usize % length
        }
    }

    fn assert_live_ranges_scalable(ranges: &[(u64, u64)], capacity: u64) {
        let mut ordered = ranges.to_vec();
        for &(offset, size) in &ordered {
            assert!(size > 0);
            assert!(offset.checked_add(size).unwrap() <= capacity);
        }
        ordered.sort_unstable_by_key(|&(offset, _)| offset);
        for adjacent in ordered.windows(2) {
            assert!(adjacent[0].0 + adjacent[0].1 <= adjacent[1].0);
        }
    }

    fn assert_live_descriptors_scalable(live: &[ReplicaDescriptor], capacity: u64) {
        let ranges = live
            .iter()
            .map(|replica| (replica.offset, replica.size))
            .collect::<Vec<_>>();
        assert_live_ranges_scalable(&ranges, capacity);
    }

    fn run_small_random_allocation_phase(
        allocator: &mut SegmentAllocator,
        segment_id: Uuid,
        live: &mut Vec<ReplicaDescriptor>,
        rng: &mut DeterministicRng,
    ) {
        for _ in 0..100 {
            let requested = rng.one_through(1_024);
            live.push(allocate_facade_exact(allocator, segment_id, MIB, requested));
            assert_live_descriptors_scalable(live, MIB);

            if rng.next() & 1 == 0 {
                let removed = live.swap_remove(rng.index(live.len()));
                release_facade_exact(allocator, removed);
                assert_live_descriptors_scalable(live, MIB);
            }
        }
    }

    fn run_random_replacement_facade_step(
        allocator: &mut SegmentAllocator,
        segment_id: Uuid,
        live: &mut Vec<ReplicaDescriptor>,
        capacity: u64,
        rng: &mut DeterministicRng,
    ) {
        let requested = rng.one_through(capacity / 100);
        if let Ok(replica) = allocator.allocate_from_segment_id(segment_id, requested) {
            assert_eq!(replica.size, requested);
            assert!(replica.offset.checked_add(replica.size).unwrap() <= capacity);
            live.push(replica);
            assert_live_descriptors_scalable(live, capacity);
        }

        assert!(!live.is_empty());
        let removed = live.swap_remove(rng.index(live.len()));
        let replacement_size = removed.size;
        release_facade_exact(allocator, removed);
        live.push(allocate_facade_exact(
            allocator,
            segment_id,
            capacity,
            replacement_size,
        ));
        assert_live_descriptors_scalable(live, capacity);
    }

    fn random_replacement_step(
        state: &mut SegmentState,
        live: &mut Vec<(u64, u64)>,
        capacity: u64,
        maximum_size: u64,
        rng: &mut DeterministicRng,
    ) {
        let requested = rng.one_through(maximum_size);
        if let Some(allocation) = state.allocate(requested) {
            assert_eq!(allocation.1, requested);
            live.push(allocation);
            assert_live_ranges_scalable(live, capacity);
        }

        assert!(!live.is_empty());
        let removed = live.swap_remove(rng.index(live.len()));
        release_exact(state, removed);
        let replacement = allocate_exact(state, capacity, removed.1);
        live.push(replacement);
        assert_live_ranges_scalable(live, capacity);
    }

    #[test]
    fn cpp_parity_1023_and_1024_pack_into_2048() {
        let mut state = offset_state(2_048);
        let first = allocate_exact(&mut state, 2_048, 1_023);
        let second = allocate_exact(&mut state, 2_048, 1_024);

        assert_live_ranges(&[first, second], 2_048);
    }

    #[test]
    fn cpp_parity_sixteen_byte_exact_fit_then_exhaustion() {
        let mut state = offset_state(16);

        assert_eq!(allocate_exact(&mut state, 16, 16), (0, 16));
        assert_eq!(state.allocate(1), None);
    }

    #[test]
    fn cpp_parity_1023_succeeds_in_1024_capacity() {
        let mut state = offset_state(1_024);

        assert_eq!(allocate_exact(&mut state, 1_024, 1_023), (0, 1_023));
    }

    #[test]
    fn cpp_parity_power_of_two_sizes_1_through_1024_in_2048() {
        let mut state = offset_state(2_048);

        for size in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024] {
            let allocation = allocate_exact(&mut state, 2_048, size);
            release_exact(&mut state, allocation);
        }
    }

    #[test]
    fn cpp_parity_100_200_500_live_ranges_are_valid() {
        let mut state = offset_state(MIB);
        let ranges = [
            allocate_exact(&mut state, MIB, 100),
            allocate_exact(&mut state, MIB, 200),
            allocate_exact(&mut state, MIB, 500),
        ];

        assert_live_ranges(&ranges, MIB);
    }

    #[test]
    fn cpp_parity_up_to_100_live_64_byte_bin_allocations() {
        let mut state = offset_state(MIB);
        let mut ranges = Vec::with_capacity(100);

        for _ in 0..100 {
            let Some(allocation) = state.allocate(64) else {
                break;
            };
            assert_eq!(allocation.1, 64);
            ranges.push(allocation);
            assert_live_ranges(&ranges, MIB);
        }

        assert!(!ranges.is_empty());
    }

    #[test]
    fn cpp_parity_exact_nineteen_bin_edge_sizes() {
        let mut state = offset_state(MIB);

        for size in [
            1, 2, 3, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 511, 512, 1_023, 1_024,
        ] {
            let allocation = allocate_exact(&mut state, MIB, size);
            release_exact(&mut state, allocation);
        }
    }

    #[test]
    fn cpp_parity_exact_seven_scale_mixed_live_set() {
        let mut state = offset_state(MIB);
        let ranges = [16, 64, 256, 1_024, 4_096, 16_384, 65_536]
            .map(|size| allocate_exact(&mut state, MIB, size));

        assert_live_ranges(&ranges, MIB);
    }

    #[test]
    fn cpp_parity_exact_nine_nonaligned_sizes() {
        let mut state = offset_state(MIB);

        for size in [17, 33, 65, 129, 257, 513, 1_025, 2_049, 4_097] {
            let allocation = allocate_exact(&mut state, MIB, size);
            release_exact(&mut state, allocation);
        }
    }

    #[test]
    fn cpp_parity_full_capacity_release_and_reuse() {
        const GIB: u64 = 1024 * MIB;
        let mut state = offset_state(GIB);

        let first = allocate_exact(&mut state, GIB, GIB);
        assert_eq!(first, (0, GIB));
        assert_ne!(state.segment.base + first.0, u64::MAX);
        assert_eq!(state.allocate(GIB), None);

        release_exact(&mut state, first);
        let reused = allocate_exact(&mut state, GIB, GIB);
        assert_eq!(reused, (0, GIB));
        assert_ne!(state.segment.base + reused.0, u64::MAX);
    }

    #[test]
    fn cpp_parity_all_239_bin_sizes_exact_fit() {
        for capacity in CPP_NONZERO_BIN_SIZES {
            let mut state = offset_state(capacity);

            assert_eq!(
                allocate_exact(&mut state, capacity, capacity),
                (0, capacity)
            );
        }
    }

    #[test]
    fn cpp_parity_storage_report_decreases_after_thousand_bytes() {
        const GIB: u64 = 1024 * MIB;
        let (mut allocator, segment_id) = offset_allocator_facade(GIB);
        let initial = allocator
            .offset_allocator_report(&segment_id)
            .expect("offset report");

        assert!(initial.total_free_space > 0);
        assert!(initial.largest_free_region > 0);
        let allocation = allocate_facade_exact(&mut allocator, segment_id, GIB, 1_000);
        assert_eq!(allocation.size, 1_000);

        let after = allocator
            .offset_allocator_report(&segment_id)
            .expect("offset report");
        assert!(after.total_free_space < initial.total_free_space);
    }

    #[test]
    fn cpp_parity_176_bins_accept_ten_near_capacity_reuses() {
        let bins = CPP_NONZERO_BIN_SIZES
            .into_iter()
            .filter(|&size| size >= 1_024)
            .collect::<Vec<_>>();
        assert_eq!(bins.len(), 176);

        for bin_size in bins {
            let capacity = bin_size + 10;
            let (mut allocator, segment_id) = offset_allocator_facade(capacity);
            assert_eq!(
                allocator
                    .offset_allocator_report(&segment_id)
                    .expect("offset report")
                    .total_free_space,
                capacity
            );

            for delta in (1..=10).rev() {
                let allocation =
                    allocate_facade_exact(&mut allocator, segment_id, capacity, bin_size - delta);
                release_facade_exact(&mut allocator, allocation);
            }
        }
    }

    #[test]
    fn cpp_parity_powers_30_through_40_large_capacity_boundaries() {
        for shift in 30..=40 {
            let capacity = 1_u64 << shift;
            let (mut allocator, segment_id) = offset_allocator_facade(capacity);
            assert_eq!(
                allocator
                    .offset_allocator_report(&segment_id)
                    .expect("offset report")
                    .largest_free_region,
                capacity
            );

            let one = allocate_facade_exact(&mut allocator, segment_id, capacity, 1);
            release_facade_exact(&mut allocator, one);
            let capacity_minus_one =
                allocate_facade_exact(&mut allocator, segment_id, capacity, capacity - 1);
            release_facade_exact(&mut allocator, capacity_minus_one);
            let full = allocate_facade_exact(&mut allocator, segment_id, capacity, capacity);
            assert_eq!((full.offset, full.size), (0, capacity));
        }
    }

    #[test]
    fn cpp_parity_100_random_huge_largest_regions_are_allocatable() {
        const MINIMUM_CAPACITY: u64 = (1_u64 << 31) + 1;
        const MAXIMUM_CAPACITY: u64 = 1_u64 << 40;
        let mut rng = DeterministicRng(0x61e2_4d7a_98b3_c50f);

        for _ in 0..100 {
            let capacity = rng.inclusive(MINIMUM_CAPACITY, MAXIMUM_CAPACITY);
            let (mut allocator, segment_id) = offset_allocator_facade(capacity);
            let largest = allocator
                .offset_allocator_report(&segment_id)
                .expect("offset report")
                .largest_free_region;

            assert!(largest > capacity / 2);
            let allocation = allocate_facade_exact(&mut allocator, segment_id, capacity, largest);
            assert_eq!((allocation.offset, allocation.size), (0, largest));
        }
    }

    #[test]
    fn cpp_parity_max_1000_allows_exactly_999_live_handles() {
        const GIB: u64 = 1024 * MIB;
        let (mut allocator, segment_id) = limited_offset_allocator_facade(GIB, 1_000);
        let mut handles = Vec::with_capacity(999);

        for _ in 0..999 {
            handles.push(allocate_facade_exact(
                &mut allocator,
                segment_id,
                GIB,
                1_024,
            ));
        }
        let ranges = handles
            .iter()
            .map(|replica| (replica.offset, replica.size))
            .collect::<Vec<_>>();
        assert_live_ranges_scalable(&ranges, GIB);
        let report = allocator
            .offset_allocator_report(&segment_id)
            .expect("offset report");
        assert_eq!(report.allocation_count, 999);
        assert_eq!(report.total_free_space, GIB - 999 * 1_024);
        assert!(report.total_free_space > 0);

        assert!(matches!(
            allocator.allocate_from_segment_id(segment_id, 1_024),
            Err(SegmentAllocationError::NoAvailableHandle)
        ));
        assert_eq!(
            allocator
                .offset_allocator_report(&segment_id)
                .expect("failed allocation leaves report available"),
            report
        );
    }

    #[test]
    fn cpp_parity_handle_limit_nine_fail_tenth_reuse_one_slot() {
        let (mut allocator, segment_id) = limited_offset_allocator_facade(MIB, 10);
        let mut handles = (0..9)
            .map(|_| allocate_facade_exact(&mut allocator, segment_id, MIB, 1_024))
            .collect::<Vec<_>>();
        assert_live_ranges_scalable(
            &handles
                .iter()
                .map(|replica| (replica.offset, replica.size))
                .collect::<Vec<_>>(),
            MIB,
        );
        assert!(matches!(
            allocator.allocate_from_segment_id(segment_id, 1_024),
            Err(SegmentAllocationError::NoAvailableHandle)
        ));

        let released = handles.pop().expect("ninth handle");
        release_facade_exact(&mut allocator, released);
        handles.push(allocate_facade_exact(
            &mut allocator,
            segment_id,
            MIB,
            1_024,
        ));
        assert_live_ranges_scalable(
            &handles
                .iter()
                .map(|replica| (replica.offset, replica.size))
                .collect::<Vec<_>>(),
            MIB,
        );
        let report = allocator
            .offset_allocator_report(&segment_id)
            .expect("offset report");
        assert_eq!(report.allocation_count, 9);
        assert!(report.total_free_space > 0);
    }

    #[test]
    fn offset_node_limit_requires_coalescing_before_fragmented_capacity_reopens() {
        let (mut allocator, segment_id) = limited_offset_allocator_facade(1_000, 4);
        let first = allocate_facade_exact(&mut allocator, segment_id, 1_000, 100);
        let middle = allocate_facade_exact(&mut allocator, segment_id, 1_000, 100);
        let last = allocate_facade_exact(&mut allocator, segment_id, 1_000, 100);

        assert!(matches!(
            allocator.allocate_from_segment_id(segment_id, 100),
            Err(SegmentAllocationError::NoAvailableHandle)
        ));

        release_facade_exact(&mut allocator, middle);
        assert!(matches!(
            allocator.allocate_from_segment_id(segment_id, 100),
            Err(SegmentAllocationError::NoAvailableHandle)
        ));

        release_facade_exact(&mut allocator, last);
        let replacement = allocate_facade_exact(&mut allocator, segment_id, 1_000, 100);
        assert_live_ranges_scalable(
            &[
                (first.offset, first.size),
                (replacement.offset, replacement.size),
            ],
            1_000,
        );
    }

    #[test]
    fn offset_limited_registration_rejects_duplicate_segment_without_losing_allocations() {
        let (mut allocator, segment_id) = limited_offset_allocator_facade(1_000, 4);
        let live = allocate_facade_exact(&mut allocator, segment_id, 1_000, 100);
        let duplicate = Segment {
            id: segment_id,
            name: "replacement-must-not-win".to_string(),
            base: 99_999,
            size: 2_000,
            te_endpoint: "127.0.0.1:54321".to_string(),
            protocol: "tcp".to_string(),
            host_id: "replacement-host".to_string(),
        };

        assert!(
            allocator
                .try_add_segment(duplicate, 0, Uuid::new_v4())
                .is_err()
        );
        release_facade_exact(&mut allocator, live);
        let report = allocator
            .offset_allocator_report(&segment_id)
            .expect("original segment remains registered");
        assert_eq!(report.capacity, 1_000);
        assert_eq!(report.allocation_count, 0);
        assert_eq!(report.total_free_space, 1_000);
    }

    #[test]
    fn offset_node_limit_configuration_rejects_invalid_modes_and_default_stays_unlimited() {
        assert!(
            SegmentAllocator::new()
                .try_with_offset_max_allocation_nodes(Some(0))
                .is_err()
        );
        assert!(
            SegmentAllocator::new()
                .with_memory_allocator(MemoryAllocatorKind::CachelibLike)
                .try_with_offset_max_allocation_nodes(Some(4))
                .is_err()
        );
        assert!(
            std::panic::catch_unwind(|| {
                SegmentAllocator::new()
                    .try_with_offset_max_allocation_nodes(Some(4))
                    .expect("valid Offset node budget")
                    .with_memory_allocator(MemoryAllocatorKind::CachelibLike)
            })
            .is_err(),
            "changing a limited Offset allocator to Cachelib must reject the invalid combination"
        );
        assert!(
            SegmentAllocator::new()
                .try_with_offset_max_allocation_nodes(Some(4))
                .expect("valid Offset node budget")
                .try_with_memory_allocator(MemoryAllocatorKind::CachelibLike)
                .is_err()
        );

        let (mut ordinary, segment_id) = offset_allocator_facade(16);
        let handles = (0..16)
            .map(|_| allocate_facade_exact(&mut ordinary, segment_id, 16, 1))
            .collect::<Vec<_>>();
        assert_eq!(handles.len(), 16);
        assert_eq!(ordinary.snapshot_config().offset_max_allocation_nodes, None);
    }

    #[test]
    fn offset_limited_public_registration_does_not_replace_duplicate_segment() {
        let (mut allocator, segment_id) = limited_offset_allocator_facade(1_000, 4);
        let live = allocate_facade_exact(&mut allocator, segment_id, 1_000, 100);
        allocator.add_segment(
            Segment {
                id: segment_id,
                name: "replacement-must-not-win".to_string(),
                base: 99_999,
                size: 2_000,
                te_endpoint: "127.0.0.1:54321".to_string(),
                protocol: "tcp".to_string(),
                host_id: "replacement-host".to_string(),
            },
            0,
            Uuid::new_v4(),
        );

        release_facade_exact(&mut allocator, live);
        let report = allocator
            .offset_allocator_report(&segment_id)
            .expect("original segment remains registered");
        assert_eq!(report.capacity, 1_000);
        assert_eq!(report.allocation_count, 0);
        assert_eq!(report.total_free_space, 1_000);
    }

    #[test]
    fn offset_node_limit_survives_serialized_config_and_descriptor_restore() {
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: segment_id,
            name: "offset-limit-restore".to_string(),
            base: 16 * 1024,
            size: 1_000,
            te_endpoint: "127.0.0.1:12345".to_string(),
            protocol: "tcp".to_string(),
            host_id: "offset-limit-host".to_string(),
        };
        let mut original = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(4))
            .expect("valid Offset node budget");
        original
            .try_add_segment(segment.clone(), 0, client_id)
            .expect("fresh segment");
        let live = (0..3)
            .map(|_| allocate_facade_exact(&mut original, segment_id, 1_000, 100))
            .collect::<Vec<_>>();

        let encoded = rmp_serde::to_vec_named(&original.snapshot_config())
            .expect("serialize allocator config");
        let restored_config: AllocatorSnapshotConfig =
            rmp_serde::from_slice(&encoded).expect("deserialize allocator config");
        let mut restored = SegmentAllocator::new()
            .with_strategy(restored_config.allocation_strategy)
            .with_memory_allocator(restored_config.memory_allocator_kind)
            .try_with_offset_max_allocation_nodes(restored_config.offset_max_allocation_nodes)
            .expect("restored Offset node budget");
        restored
            .restore_segment(segment, client_id, &live)
            .expect("descriptor-driven restore");

        assert!(matches!(
            restored.allocate_from_segment_id(segment_id, 100),
            Err(SegmentAllocationError::NoAvailableHandle)
        ));
        release_facade_exact(&mut restored, live[2].clone());
        allocate_facade_exact(&mut restored, segment_id, 1_000, 100);
    }

    #[test]
    fn offset_restore_rejects_snapshot_whose_partitions_exceed_node_budget() {
        let segment_id = Uuid::new_v4();
        let client_id = Uuid::new_v4();
        let segment = Segment {
            id: segment_id,
            name: "offset-limit-overfull-restore".to_string(),
            base: 16 * 1024,
            size: 1_000,
            te_endpoint: "127.0.0.1:12345".to_string(),
            protocol: "tcp".to_string(),
            host_id: "offset-limit-host".to_string(),
        };
        let mut source = SegmentAllocator::new();
        source
            .try_add_segment(segment.clone(), 0, client_id)
            .expect("fresh source segment");
        let live = (0..4)
            .map(|_| allocate_facade_exact(&mut source, segment_id, 1_000, 100))
            .collect::<Vec<_>>();
        let mut restored = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(4))
            .expect("valid Offset node budget");

        assert!(restored.restore_segment(segment, client_id, &live).is_err());
        assert!(restored.offset_allocator_report(&segment_id).is_none());
    }

    #[test]
    fn cpp_parity_empty_allocator_snapshot_short_long_not_equal() {
        let maximum = 10_000;
        let (allocator, segment_id) = limited_offset_allocator_facade(MIB, maximum);
        let encoded = allocator
            .serialize_offset_segment(&segment_id)
            .expect("serialize empty Offset segment");
        let original_report = allocator
            .offset_allocator_report(&segment_id)
            .expect("original Offset report");

        let mut restored = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(maximum))
            .expect("valid Offset node budget");
        let rebound = restored
            .restore_offset_segment_snapshot(&encoded)
            .expect("restore clean empty snapshot");
        assert!(rebound.is_empty());
        assert_eq!(
            restored
                .serialize_offset_segment(&segment_id)
                .expect("reserialize restored segment"),
            encoded
        );
        assert_eq!(
            restored
                .offset_allocator_report(&segment_id)
                .expect("restored Offset report"),
            original_report
        );

        let shortened = &encoded[..encoded.len() - 1];
        let mut short_target = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(maximum))
            .expect("valid Offset node budget");
        assert!(
            short_target
                .restore_offset_segment_snapshot(shortened)
                .is_err()
        );
        assert!(short_target.offset_allocator_report(&segment_id).is_none());

        let mut extended = encoded.clone();
        extended.push(0);
        let mut long_target = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(maximum))
            .expect("valid Offset node budget");
        assert!(
            long_target
                .restore_offset_segment_snapshot(&extended)
                .is_err()
        );
        assert!(long_target.offset_allocator_report(&segment_id).is_none());

        let mut second = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(maximum))
            .expect("valid Offset node budget");
        assert!(
            second
                .restore_offset_segment_snapshot(&encoded)
                .expect("second clean restore")
                .is_empty()
        );
    }

    #[test]
    fn cpp_parity_one_live_allocation_snapshot_rebinds_and_frees() {
        let maximum = 10_000;
        let (mut allocator, segment_id) = limited_offset_allocator_facade(MIB, maximum);
        let live = allocate_facade_exact(&mut allocator, segment_id, MIB, 1_024);
        assert_eq!((live.offset, live.size), (0, 1_024));
        let encoded = allocator
            .serialize_offset_segment(&segment_id)
            .expect("serialize one-allocation Offset segment");
        let original_report = allocator
            .offset_allocator_report(&segment_id)
            .expect("original Offset report");

        let mut restored = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(maximum))
            .expect("valid Offset node budget");
        let rebound = restored
            .restore_offset_segment_snapshot(&encoded)
            .expect("restore clean one-allocation snapshot");
        assert_eq!(rebound.len(), 1);
        assert_eq!((rebound[0].offset, rebound[0].size), (0, 1_024));
        assert_eq!(
            restored
                .serialize_offset_segment(&segment_id)
                .expect("reserialize restored segment"),
            encoded
        );
        assert_eq!(
            restored
                .offset_allocator_report(&segment_id)
                .expect("restored Offset report"),
            original_report
        );

        let shortened = &encoded[..encoded.len() - 1];
        let mut short_target = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(maximum))
            .expect("valid Offset node budget");
        assert!(
            short_target
                .restore_offset_segment_snapshot(shortened)
                .is_err()
        );
        assert!(short_target.offset_allocator_report(&segment_id).is_none());

        let mut extended = encoded.clone();
        extended.push(0);
        let mut long_target = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(maximum))
            .expect("valid Offset node budget");
        assert!(
            long_target
                .restore_offset_segment_snapshot(&extended)
                .is_err()
        );
        assert!(long_target.offset_allocator_report(&segment_id).is_none());

        let mut second = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(maximum))
            .expect("valid Offset node budget");
        let mut rebound = second
            .restore_offset_segment_snapshot(&encoded)
            .expect("second clean restore");
        assert_eq!(rebound.len(), 1);
        release_facade_exact(&mut second, rebound.pop().expect("rebound descriptor"));
        let report = second
            .offset_allocator_report(&segment_id)
            .expect("fully released Offset report");
        assert_eq!(report.allocated_bytes, 0);
        assert_eq!(report.allocation_count, 0);
        assert_eq!(report.total_free_space, MIB);
        assert_eq!(report.largest_free_region, MIB);
    }

    #[test]
    fn cpp_parity_100_random_huge_snapshots_preserve_live_state() {
        const MAXIMUM_NODES: u64 = 10_000;
        let mut rng = DeterministicRng(0x5a17_5eed_cafe_f00d);

        for _ in 0..100 {
            let capacity = rng.inclusive((1_u64 << 31) + 1, 1_u64 << 40);
            let (mut source, segment_id) = limited_offset_allocator_facade(capacity, MAXIMUM_NODES);
            let mut live = Vec::new();

            for _ in 0..200 {
                let requested = rng.one_through(capacity / 100);
                if let Ok(replica) = source.allocate_from_segment_id(segment_id, requested) {
                    assert_eq!(replica.size, requested);
                    assert!(replica.offset.checked_add(replica.size).unwrap() <= capacity);
                    live.push(replica);
                    assert_live_descriptors_scalable(&live, capacity);
                }

                assert!(!live.is_empty());
                let removed = live.swap_remove(rng.index(live.len()));
                let replacement_size = removed.size;
                release_facade_exact(&mut source, removed);
                live.push(allocate_facade_exact(
                    &mut source,
                    segment_id,
                    capacity,
                    replacement_size,
                ));
                assert_live_descriptors_scalable(&live, capacity);
            }

            let encoded = source
                .serialize_offset_segment(&segment_id)
                .expect("serialize random huge Offset segment");
            let source_report = source
                .offset_allocator_report(&segment_id)
                .expect("source random huge Offset report");

            let mut restored = SegmentAllocator::new()
                .try_with_offset_max_allocation_nodes(Some(MAXIMUM_NODES))
                .expect("valid Offset node budget");
            let rebound = restored
                .restore_offset_segment_snapshot(&encoded)
                .expect("restore random huge Offset snapshot");
            assert_live_descriptors_scalable(&rebound, capacity);
            assert_eq!(rebound.len(), live.len());
            assert_eq!(
                restored
                    .serialize_offset_segment(&segment_id)
                    .expect("reserialize random huge Offset segment"),
                encoded
            );
            assert_eq!(
                restored
                    .offset_allocator_report(&segment_id)
                    .expect("restored random huge Offset report"),
                source_report
            );

            let mut short_target = SegmentAllocator::new()
                .try_with_offset_max_allocation_nodes(Some(MAXIMUM_NODES))
                .expect("valid Offset node budget");
            assert!(
                short_target
                    .restore_offset_segment_snapshot(&encoded[..encoded.len() - 1])
                    .is_err()
            );
            assert!(short_target.offset_allocator_report(&segment_id).is_none());

            let mut extended = encoded.clone();
            extended.push(0);
            let mut long_target = SegmentAllocator::new()
                .try_with_offset_max_allocation_nodes(Some(MAXIMUM_NODES))
                .expect("valid Offset node budget");
            assert!(
                long_target
                    .restore_offset_segment_snapshot(&extended)
                    .is_err()
            );
            assert!(long_target.offset_allocator_report(&segment_id).is_none());

            let mut second = SegmentAllocator::new()
                .try_with_offset_max_allocation_nodes(Some(MAXIMUM_NODES))
                .expect("valid Offset node budget");
            let rebound = second
                .restore_offset_segment_snapshot(&encoded)
                .expect("second clean random huge Offset restore");
            assert_eq!(rebound.len(), live.len());
            for replica in rebound {
                release_facade_exact(&mut second, replica);
            }
            let report = second
                .offset_allocator_report(&segment_id)
                .expect("fully released random huge Offset report");
            assert_eq!(report.allocated_bytes, 0);
            assert_eq!(report.allocation_count, 0);
            assert_eq!(report.capacity, capacity);
            assert_eq!(report.total_free_space, capacity);
            assert_eq!(report.largest_free_region, capacity);
        }
    }

    #[test]
    fn cpp_parity_random_allocation_continues_after_snapshot_restore() {
        const MAXIMUM_NODES: u64 = 10_000;
        let (mut source, segment_id) = limited_offset_allocator_facade(MIB, MAXIMUM_NODES);
        let mut live = Vec::new();
        let mut first_phase_rng = DeterministicRng(0xc001_d00d_0000_0001);
        run_small_random_allocation_phase(&mut source, segment_id, &mut live, &mut first_phase_rng);

        let encoded = source
            .serialize_offset_segment(&segment_id)
            .expect("serialize first random allocation phase");
        let source_report = source
            .offset_allocator_report(&segment_id)
            .expect("source continued-allocation Offset report");
        let survivor_count = live.len();
        let mut restored = SegmentAllocator::new()
            .try_with_offset_max_allocation_nodes(Some(MAXIMUM_NODES))
            .expect("valid Offset node budget");
        live = restored
            .restore_offset_segment_snapshot(&encoded)
            .expect("restore before second random allocation phase");
        assert_eq!(live.len(), survivor_count);
        assert_live_descriptors_scalable(&live, MIB);
        assert_eq!(
            restored
                .serialize_offset_segment(&segment_id)
                .expect("reserialize before second random allocation phase"),
            encoded
        );
        assert_eq!(
            restored
                .offset_allocator_report(&segment_id)
                .expect("restored continued-allocation Offset report"),
            source_report
        );

        let mut second_phase_rng = DeterministicRng(0xc001_d00d_0000_0002);
        run_small_random_allocation_phase(
            &mut restored,
            segment_id,
            &mut live,
            &mut second_phase_rng,
        );

        for replica in live {
            release_facade_exact(&mut restored, replica);
        }
        let report = restored
            .offset_allocator_report(&segment_id)
            .expect("fully released continued-allocation report");
        assert_eq!(report.allocated_bytes, 0);
        assert_eq!(report.allocation_count, 0);
        assert_eq!(report.total_free_space, MIB);
        assert_eq!(report.largest_free_region, MIB);
    }

    #[test]
    fn cpp_parity_ten_huge_allocators_survive_1000_replacements_and_rebind() {
        const MAXIMUM_NODES: u64 = 10_000;
        let mut rng = DeterministicRng(0x0ff5_e7aa_110c_a7e5);

        for _ in 0..10 {
            let capacity = rng.inclusive((1_u64 << 31) + 1, 1_u64 << 40);
            let (mut source, segment_id) = limited_offset_allocator_facade(capacity, MAXIMUM_NODES);
            let mut live = Vec::new();

            for _ in 0..10 {
                for _ in 0..100 {
                    run_random_replacement_facade_step(
                        &mut source,
                        segment_id,
                        &mut live,
                        capacity,
                        &mut rng,
                    );
                }
            }

            let encoded = source
                .serialize_offset_segment(&segment_id)
                .expect("serialize chained huge Offset segment");
            let source_report = source
                .offset_allocator_report(&segment_id)
                .expect("source chained huge Offset report");
            let mut restored = SegmentAllocator::new()
                .try_with_offset_max_allocation_nodes(Some(MAXIMUM_NODES))
                .expect("valid Offset node budget");
            let rebound = restored
                .restore_offset_segment_snapshot(&encoded)
                .expect("restore chained huge Offset snapshot");
            assert_eq!(rebound.len(), live.len());
            assert_live_descriptors_scalable(&rebound, capacity);
            assert_eq!(
                restored
                    .serialize_offset_segment(&segment_id)
                    .expect("reserialize chained huge Offset segment"),
                encoded
            );
            assert_eq!(
                restored
                    .offset_allocator_report(&segment_id)
                    .expect("restored chained huge Offset report"),
                source_report
            );

            for replica in rebound {
                release_facade_exact(&mut restored, replica);
            }
            let report = restored
                .offset_allocator_report(&segment_id)
                .expect("fully released chained huge Offset report");
            assert_eq!(report.allocated_bytes, 0);
            assert_eq!(report.allocation_count, 0);
            assert_eq!(report.capacity, capacity);
            assert_eq!(report.total_free_space, capacity);
            assert_eq!(report.largest_free_region, capacity);
        }
    }

    #[test]
    fn cpp_parity_metrics_exact_five_snapshot_lifecycle() {
        let (mut allocator, segment_id) = offset_allocator_facade(MIB);
        let initial = allocator
            .offset_allocator_report(&segment_id)
            .expect("offset report");
        assert_eq!(
            (
                initial.allocated_bytes,
                initial.allocation_count,
                initial.capacity,
                initial.total_free_space,
            ),
            (0, 0, MIB, MIB)
        );
        assert!(initial.largest_free_region > 0);

        let first = allocate_facade_exact(&mut allocator, segment_id, MIB, 1_024);
        let after_first = allocator
            .offset_allocator_report(&segment_id)
            .expect("offset report");
        assert_eq!(
            (
                after_first.allocated_bytes,
                after_first.allocation_count,
                after_first.capacity,
                after_first.total_free_space,
            ),
            (1_024, 1, MIB, MIB - 1_024)
        );
        assert!(after_first.total_free_space < initial.total_free_space);

        let second = allocate_facade_exact(&mut allocator, segment_id, MIB, 2_048);
        assert_live_ranges(
            &[(first.offset, first.size), (second.offset, second.size)],
            MIB,
        );
        let after_second = allocator
            .offset_allocator_report(&segment_id)
            .expect("offset report");
        assert_eq!(
            (
                after_second.allocated_bytes,
                after_second.allocation_count,
                after_second.capacity,
                after_second.total_free_space,
            ),
            (3_072, 2, MIB, MIB - 3_072)
        );
        assert!(after_second.total_free_space < after_first.total_free_space);

        release_facade_exact(&mut allocator, first);
        let after_first_release = allocator
            .offset_allocator_report(&segment_id)
            .expect("offset report");
        assert_eq!(
            (
                after_first_release.allocated_bytes,
                after_first_release.allocation_count,
                after_first_release.capacity,
                after_first_release.total_free_space,
            ),
            (2_048, 1, MIB, MIB - 2_048)
        );
        assert!(after_first_release.total_free_space > after_second.total_free_space);

        release_facade_exact(&mut allocator, second);
        let after_all_release = allocator
            .offset_allocator_report(&segment_id)
            .expect("offset report");
        assert_eq!(
            (
                after_all_release.allocated_bytes,
                after_all_release.allocation_count,
                after_all_release.capacity,
                after_all_release.total_free_space,
                after_all_release.largest_free_region,
            ),
            (0, 0, MIB, MIB, MIB)
        );
    }

    #[test]
    fn cpp_parity_1023_reallocates_beside_live_16() {
        let mut state = offset_state(2_048);
        let first = allocate_exact(&mut state, 2_048, 1_023);
        let retained = allocate_exact(&mut state, 2_048, 16);
        assert_live_ranges(&[first, retained], 2_048);

        release_exact(&mut state, first);
        let replacement = allocate_exact(&mut state, 2_048, 1_023);
        assert_live_ranges(&[retained, replacement], 2_048);
    }

    #[test]
    fn cpp_parity_one_mib_exact_fit_starts_zero_and_exhausts() {
        let mut state = offset_state(MIB);

        assert_eq!(allocate_exact(&mut state, MIB, MIB), (0, MIB));
        assert_eq!(state.allocate(1), None);
    }

    #[test]
    fn cpp_parity_middle_first_third_release_coalesces_full_mib() {
        let mut state = offset_state(MIB);
        let ranges = [
            allocate_exact(&mut state, MIB, 1_024),
            allocate_exact(&mut state, MIB, 1_024),
            allocate_exact(&mut state, MIB, 1_024),
        ];
        assert_live_ranges(&ranges, MIB);

        release_exact(&mut state, ranges[1]);
        release_exact(&mut state, ranges[0]);
        release_exact(&mut state, ranges[2]);
        assert_eq!(allocate_exact(&mut state, MIB, MIB), (0, MIB));
    }

    #[test]
    fn cpp_parity_ten_mixed_cycles_restore_full_mib() {
        let mut state = offset_state(MIB);

        for _ in 0..10 {
            let ranges = [64, 128, 256, 512, 1_024, 2_048, 4_096]
                .map(|size| allocate_exact(&mut state, MIB, size));
            assert_live_ranges(&ranges, MIB);
            for allocation in ranges {
                release_exact(&mut state, allocation);
            }
        }

        assert_eq!(allocate_exact(&mut state, MIB, MIB), (0, MIB));
    }

    #[test]
    fn cpp_parity_ten_live_thousand_byte_ranges() {
        const GIB: u64 = 1024 * MIB;
        let mut state = offset_state(GIB);
        let ranges = (0..10)
            .map(|_| allocate_exact(&mut state, GIB, 1_000))
            .collect::<Vec<_>>();

        assert_eq!(ranges.len(), 10);
        assert_live_ranges(&ranges, GIB);
    }

    #[test]
    fn cpp_parity_exact_eight_size_live_set() {
        const GIB: u64 = 1024 * MIB;
        let mut state = offset_state(GIB);
        let ranges = [100, 500, 1_000, 2_000, 50, 1_500, 800, 300]
            .map(|size| allocate_exact(&mut state, GIB, size));

        assert_live_ranges(&ranges, GIB);
    }

    #[test]
    fn cpp_parity_one_byte_exact_in_one_mib() {
        let mut state = offset_state(MIB);
        let allocation = allocate_exact(&mut state, MIB, 1);

        assert_eq!(allocation, (0, 1));
        assert_ne!(state.segment.base + allocation.0, u64::MAX);
    }

    #[test]
    fn cpp_parity_large_powers_one_mib_through_512_mib() {
        const GIB: u64 = 1024 * MIB;
        let mut state = offset_state(GIB);

        for size in [
            MIB,
            2 * MIB,
            4 * MIB,
            8 * MIB,
            16 * MIB,
            32 * MIB,
            64 * MIB,
            128 * MIB,
            256 * MIB,
            512 * MIB,
        ] {
            let allocation = allocate_exact(&mut state, GIB, size);
            release_exact(&mut state, allocation);
        }
    }

    #[test]
    fn cpp_parity_exact_prime_sizes_two_through_1013() {
        let mut state = offset_state(MIB);

        for size in [
            2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83,
            89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179,
            181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271,
            277, 281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379,
            383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479,
            487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599,
            601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701,
            709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823,
            827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941,
            947, 953, 967, 971, 977, 983, 991, 997, 1_009, 1_013,
        ] {
            let allocation = allocate_exact(&mut state, MIB, size);
            release_exact(&mut state, allocation);
        }
    }

    #[test]
    fn cpp_parity_exact_fibonacci_sequence_through_317811() {
        let mut state = offset_state(MIB);

        for size in [
            1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1_597, 2_584, 4_181,
            6_765, 10_946, 17_711, 28_657, 46_368, 75_025, 121_393, 196_418, 317_811,
        ] {
            let allocation = allocate_exact(&mut state, MIB, size);
            release_exact(&mut state, allocation);
        }
    }

    #[test]
    fn cpp_parity_exact_page_multiples_four_kib_through_one_mib() {
        let mut state = offset_state(MIB);

        for size in [
            4_096, 8_192, 16_384, 32_768, 65_536, 131_072, 262_144, 524_288, 1_048_576,
        ] {
            let allocation = allocate_exact(&mut state, MIB, size);
            release_exact(&mut state, allocation);
        }
    }

    #[test]
    fn cpp_parity_twenty_thousand_random_release_cycles_restore_gib() {
        const GIB: u64 = 1024 * MIB;
        let mut state = offset_state(GIB);
        let mut rng = DeterministicRng(0x8f3d_709c_625a_11e7);

        for _ in 0..20_000 {
            let requested = rng.one_through(64 * 1024);
            let allocation = allocate_exact(&mut state, GIB, requested);
            release_exact(&mut state, allocation);
        }

        assert_eq!(allocate_exact(&mut state, GIB, GIB), (0, GIB));
    }

    #[test]
    fn cpp_parity_thousand_random_live_ranges_clear_restores_4026531840() {
        const CAPACITY: u64 = 4_026_531_840;
        let mut state = offset_state(CAPACITY);
        let mut rng = DeterministicRng(0x32f1_895b_a46c_07d3);
        let mut live = Vec::with_capacity(1_000);

        for _ in 0..1_000 {
            let requested = rng.one_through(4_026_531);
            if let Some(allocation) = state.allocate(requested) {
                assert_eq!(allocation.1, requested);
                live.push(allocation);
                assert_live_ranges_scalable(&live, CAPACITY);
            }
        }

        for allocation in live {
            release_exact(&mut state, allocation);
        }
        assert_eq!(
            allocate_exact(&mut state, CAPACITY, CAPACITY),
            (0, CAPACITY)
        );
    }

    #[test]
    fn cpp_parity_two_thousand_random_same_size_replacements() {
        const CAPACITY: u64 = 4_026_531_840;
        let mut state = offset_state(CAPACITY);
        let mut rng = DeterministicRng(0xb120_9e6d_34ca_f857);
        let mut live = Vec::new();

        for _ in 0..2_000 {
            random_replacement_step(&mut state, &mut live, CAPACITY, 40_265_318, &mut rng);
        }
    }

    #[test]
    fn cpp_parity_eleven_large_power_capacities_random_replacements() {
        let mut rng = DeterministicRng(0xd51a_27c4_908e_b63f);

        for shift in 30..=40 {
            let capacity = 1_u64 << shift;
            let mut state = offset_state(capacity);
            let mut live = Vec::new();
            for _ in 0..200 {
                random_replacement_step(&mut state, &mut live, capacity, capacity / 100, &mut rng);
            }
        }
    }

    #[test]
    fn cpp_parity_hundred_random_huge_capacities_same_size_replacements() {
        const MINIMUM_CAPACITY: u64 = (1_u64 << 31) + 1;
        const MAXIMUM_CAPACITY: u64 = 1_u64 << 40;
        let mut rng = DeterministicRng(0x09b7_ec42_6d15_a830);

        for _ in 0..100 {
            let capacity = rng.inclusive(MINIMUM_CAPACITY, MAXIMUM_CAPACITY);
            let mut state = offset_state(capacity);
            let mut live = Vec::new();
            for _ in 0..200 {
                random_replacement_step(&mut state, &mut live, capacity, capacity / 100, &mut rng);
            }
        }
    }
}
