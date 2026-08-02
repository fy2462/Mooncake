use super::*;

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
            }),
            client_id: Uuid::new_v4(),
            runtime_bound: true,
        }
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
}
