//! # Buffer Allocator — RDMA 缓冲区子分配器
//!
//! 在预分配的大块内存中进行 4K 对齐的子分配，管理空闲区域并在释放时合并相邻空闲块。
//! (Sub-allocates within a pre-allocated buffer with 4K alignment, manages free regions,
//! and coalesces adjacent free blocks on deallocation.)
//!
//! ## 设计动机 (Why)
//!
//! RDMA 操作需要注册的内存缓冲区。频繁分配/释放小块内存会导致 RDMA 注册开销过大。
//! 此模块提供一个大块缓冲区 + offset-allocator 模式，将一次 RDMA 注册的内存通过 offset
//! 子分配给多个操作复用。
//!
//! ## 核心类型 (Core Types)
//!
//! - [`ClientBufferAllocator`]: 缓冲区分配器，管理内存块和空闲链表
//! - [`BufferHandle`]: RAII 句柄，Drop 时自动归还分配的区域

use parking_lot::Mutex;
use std::sync::Arc;

/// 基于 offset 的缓冲区子分配器。
/// (Offset-allocator-based sub-allocation within a buffer.)
///
/// 管理一个预分配的大块内存 (`buffer`)，通过 first-fit 策略从空闲链表中分配子区域。
/// 释放时自动合并相邻空闲块 (`coalesce`)，减少碎片。
///
/// ## 字段 (Fields)
/// - `buffer`: 底层内存块 (backing memory)，存储实际数据
/// - `free_regions`: 按 offset 排序的空闲区域列表 `[(offset, size), ...]`
/// - `total_size`: 总缓冲区大小
/// - `allocated`: 当前已分配的总字节数（用于统计和测试）
#[derive(Debug)]
pub struct ClientBufferAllocator {
    #[allow(dead_code, reason = "backing memory for allocations")]
    buffer: Vec<u8>,
    /// Sorted list of free regions (offset, size)
    /// 按 offset 升序排列的空闲区域列表
    free_regions: Vec<(usize, usize)>,
    total_size: usize,
    allocated: usize,
}

/// RAII 句柄，持有从 [`ClientBufferAllocator`] 分配的一块缓冲区区域。
/// (RAII handle for an allocated buffer region.)
///
/// ## 字段 (Fields)
/// - `offset`: 分配区域在缓冲区中的起始偏移量
/// - `size`: 分配的大小（已按 4K 对齐）
/// - `allocator`: 指向所属分配器的引用（用于 Drop 时归还内存）
///
/// ## 生命周期 (Lifecycle)
/// 当 `BufferHandle` 被 drop 时，自动将持有的区域归还给分配器的空闲链表。
/// 支持显式 `forget` 模式：用户可提前 drop handle 来归还内存。
#[derive(Debug)]
pub struct BufferHandle {
    /// 在底层缓冲区中的偏移量 (byte offset within the backing buffer)
    pub offset: usize,
    /// 已分配区域的大小（4K 对齐） (size of the allocated region, 4K-aligned)
    pub size: usize,
    /// 分配器引用，Drop 时用于归还 (reference to allocator, used on Drop for deallocation)
    allocator: Option<Arc<Mutex<ClientBufferAllocator>>>,
}

impl Drop for BufferHandle {
    fn drop(&mut self) {
        if let Some(ref allocator) = self.allocator {
            allocator.lock().deallocate(self.offset, self.size);
        }
    }
}

impl ClientBufferAllocator {
    /// 创建一个新的缓冲区分配器。
    /// (Create a new buffer allocator with the given total size.)
    ///
    /// 预分配 `total_size` 字节的零初始化内存，初始空闲链表为 `[(0, total_size)]`。
    /// 返回 `Arc<Mutex<Self>>` 以支持多线程共享。
    pub fn new(total_size: usize) -> Arc<Mutex<Self>> {
        let buffer = vec![0u8; total_size];
        Arc::new(Mutex::new(Self {
            buffer,
            free_regions: vec![(0, total_size)],
            total_size,
            allocated: 0,
        }))
    }

    /// 从分配器中分配一块大小为 `size` 的区域。
    /// (Allocate a region of `size` bytes from the allocator.)
    ///
    /// ## 策略 (Strategy)
    /// - **First-fit**: 遍历空闲链表，找到第一个足够大的区域
    /// - **4K 对齐**: 请求大小向上取整到 4K 边界 (`(size + 4095) & !4095`)
    /// - **分裂**: 如果空闲区域大于请求大小，将剩余部分保留在空闲链表中
    ///
    /// ## 返回值 (Returns)
    /// - `Some(BufferHandle)`: 分配成功
    /// - `None`: 没有足够大的连续空闲区域
    pub fn allocate(self_: &Arc<Mutex<Self>>, size: usize) -> Option<BufferHandle> {
        let mut me = self_.lock();
        // Simple first-fit with 4K alignment
        // 简单的 first-fit 策略 + 4K 对齐
        let aligned_size = (size + 4095) & !4095;
        for i in 0..me.free_regions.len() {
            let (offset, free_size) = me.free_regions[i];
            if free_size >= aligned_size {
                me.free_regions.remove(i);
                if free_size > aligned_size {
                    // 分裂：将剩余部分插回空闲链表
                    // Split: insert remainder back into free list
                    me.free_regions
                        .insert(i, (offset + aligned_size, free_size - aligned_size));
                }
                me.allocated += aligned_size;
                return Some(BufferHandle {
                    offset,
                    size: aligned_size,
                    allocator: Some(Arc::clone(self_)),
                });
            }
        }
        None
    }

    /// 释放之前分配的区域，将其归还到空闲链表。
    /// (Deallocate a previously allocated region, returning it to the free list.)
    ///
    /// 以按 offset 排序的方式插入，然后调用 `coalesce()` 合并相邻空闲块。
    fn deallocate(&mut self, offset: usize, size: usize) {
        let aligned_size = (size + 4095) & !4095;
        self.allocated = self.allocated.saturating_sub(aligned_size);

        // Insert in sorted position
        // 按 offset 升序插入到空闲链表
        let mut insert_idx = 0;
        for (i, &(o, _)) in self.free_regions.iter().enumerate() {
            if o > offset {
                break;
            }
            insert_idx = i + 1;
        }
        self.free_regions.insert(insert_idx, (offset, aligned_size));
        self.coalesce();
    }

    /// 合并相邻的空闲区域，减少外部碎片。
    /// (Coalesce adjacent free regions to reduce external fragmentation.)
    ///
    /// 遍历空闲链表，若 `region[i].offset + region[i].size == region[i+1].offset`，
    /// 则合并为一个更大的区域。
    fn coalesce(&mut self) {
        let mut i = 0;
        while i + 1 < self.free_regions.len() {
            if self.free_regions[i].0 + self.free_regions[i].1 == self.free_regions[i + 1].0 {
                let merged_size = self.free_regions[i].1 + self.free_regions[i + 1].1;
                self.free_regions[i].1 = merged_size;
                self.free_regions.remove(i + 1);
            } else {
                i += 1;
            }
        }
    }

    /// 返回缓冲区的总大小（字节）。
    /// (Returns the total buffer size in bytes.)
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    /// 返回当前已分配的字节数。
    /// (Returns the currently allocated byte count.)
    pub fn allocated(&self) -> usize {
        self.allocated
    }
}
