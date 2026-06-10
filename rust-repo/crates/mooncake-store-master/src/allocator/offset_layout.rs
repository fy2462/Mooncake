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
                Some((start, size))
            }
            SegmentLayout::Cachelib(cachelib) => allocate_cachelib(cachelib, size),
        }
    }

    /// Release the space occupied by a replica.
    /// 释放副本占用的空间。
    pub(super) fn release(&mut self, replica: &ReplicaDescriptor) -> Option<u64> {
        match &mut self.layout {
            SegmentLayout::Offset(offset) => {
                if replica.size == 0 || replica.offset >= self.segment.size {
                    return None;
                }
                let releasable = replica.size.min(self.segment.size - replica.offset);
                insert_free_range(&mut offset.free_ranges, replica.offset, releasable);
                Some(releasable)
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
