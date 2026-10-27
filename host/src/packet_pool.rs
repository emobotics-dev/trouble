//! A packet pool for allocating and freeing packet buffers with quality of service policy.
use core::cell::UnsafeCell;
#[cfg(target_has_atomic = "8")]
use core::sync::atomic::{AtomicBool, Ordering};

#[cfg(not(target_has_atomic = "8"))]
use portable_atomic::{AtomicBool, Ordering};

use crate::{config, Packet, PacketPool};

/// A packet pool holds a pool of packet buffers that can be dynamically allocated
/// and free'd.
///
/// Lock-free: a slot is claimed by a compare-and-swap on its `free` flag, and only
/// the claimer touches its buffer until it stores the flag back.
pub struct StaticPacketPool<const MTU: usize, const N: usize> {
    bufs: [UnsafeCell<[u8; MTU]>; N],
    free: [AtomicBool; N],
}

// SAFETY: a buffer is reachable only through the `PacketRef` of the one caller
// whose CAS claimed it; `free` hands it back with Release, the next claim Acquires.
unsafe impl<const MTU: usize, const N: usize> Sync for StaticPacketPool<MTU, N> {}

impl<const MTU: usize, const N: usize> Default for StaticPacketPool<MTU, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const MTU: usize, const N: usize> StaticPacketPool<MTU, N> {
    /// Create a new packet pool with the given QoS policy
    const fn new() -> Self {
        Self {
            bufs: [const { UnsafeCell::new([0; MTU]) }; N],
            free: [const { AtomicBool::new(true) }; N],
        }
    }

    fn alloc(&self) -> Option<PacketRef<MTU>> {
        let idx = self.free.iter().position(|f| {
            f.compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        })?;
        let buf = self.bufs[idx].get();
        // SAFETY: the CAS above made this slot ours alone.
        unsafe { (*buf).fill(0) };
        Some(PacketRef {
            idx,
            buf: buf.cast(),
        })
    }

    fn free(&self, p_ref: &PacketRef<MTU>) {
        self.free[p_ref.idx].store(true, Ordering::Release);
    }
}

/// Represents a reference to a packet.
#[repr(C)]
pub struct PacketRef<const MTU: usize> {
    idx: usize,
    buf: *mut u8,
}

/// Global default packet pool.
pub type DefaultPacketPool = StaticPacketPool<
    { config::DEFAULT_PACKET_POOL_MTU },
    { config::DEFAULT_PACKET_POOL_SIZE },
>;

static DEFAULT_POOL: StaticPacketPool<
    { config::DEFAULT_PACKET_POOL_MTU },
    { config::DEFAULT_PACKET_POOL_SIZE },
> = StaticPacketPool::new();

impl PacketPool for DefaultPacketPool {
    type Packet = DefaultPacket;
    const MTU: usize = { config::DEFAULT_PACKET_POOL_MTU };
    fn capacity() -> usize {
        config::DEFAULT_PACKET_POOL_SIZE
    }

    fn allocate() -> Option<DefaultPacket> {
        DEFAULT_POOL.alloc().map(|p| DefaultPacket {
            p_ref: p,
            pool: &DEFAULT_POOL,
        })
    }
}

/// Type representing the packet from the default packet pool.
pub struct DefaultPacket {
    p_ref: PacketRef<{ config::DEFAULT_PACKET_POOL_MTU }>,
    pool: &'static DefaultPacketPool,
}

impl Packet for DefaultPacket {}
impl AsRef<[u8]> for DefaultPacket {
    fn as_ref(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.p_ref.buf, config::DEFAULT_PACKET_POOL_MTU) }
    }
}

impl AsMut<[u8]> for DefaultPacket {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { core::slice::from_raw_parts_mut(self.p_ref.buf, config::DEFAULT_PACKET_POOL_MTU) }
    }
}

impl Drop for DefaultPacket {
    fn drop(&mut self) {
        self.pool.free(&self.p_ref);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_qos() {
        let pool: StaticPacketPool<27, 8> = StaticPacketPool::new();

        let a1 = pool.alloc();
        assert!(a1.is_some());
        let a2 = pool.alloc();
        assert!(a2.is_some());
        let a3 = pool.alloc();
        assert!(a3.is_some());
        let a4 = pool.alloc();
        assert!(a4.is_some());
        let a5 = pool.alloc();
        assert!(a5.is_some());
        let a6 = pool.alloc();
        assert!(a6.is_some());
        let a7 = pool.alloc();
        assert!(a7.is_some());

        let b1 = pool.alloc();
        assert!(b1.is_some());

        let b2 = pool.alloc();
        assert!(b2.is_none());
    }

    #[test]
    fn freed_slot_is_reclaimed_zeroed() {
        let pool: StaticPacketPool<4, 1> = StaticPacketPool::new();
        let a = pool.alloc().unwrap();
        // SAFETY: `a` owns the slot.
        unsafe { a.buf.write(0xAA) };
        assert!(pool.alloc().is_none());
        pool.free(&a);
        let b = pool.alloc().unwrap();
        assert_eq!(b.idx, a.idx);
        // SAFETY: `b` owns the slot.
        assert_eq!(unsafe { b.buf.read() }, 0);
    }
}
