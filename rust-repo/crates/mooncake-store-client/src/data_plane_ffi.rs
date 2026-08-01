//! Centralized unsafe boundary for caller-owned accelerator memory.
//!
//! Code under `client/` must use the safe capability and backend interfaces
//! from this module instead of calling raw Transfer Engine FFI functions.

use mooncake_store_core::StoreError;
use mooncake_store_core::error::StoreResult;
use std::ffi::c_void;
use transfer_engine_ffi::PointerMemoryType;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ForeignMemoryRegion {
    address: usize,
    len: usize,
}

impl ForeignMemoryRegion {
    /// Captures the safety promise made by a caller of a pointer-based client
    /// API. This function is crate-private so safe public APIs cannot create
    /// capabilities for arbitrary addresses.
    pub(crate) fn from_caller_owned_raw(pointer: *mut c_void, len: usize) -> Self {
        Self {
            address: pointer as usize,
            len,
        }
    }

    fn as_ptr(self) -> *const c_void {
        self.address as *const c_void
    }
    fn as_mut_ptr(self) -> *mut c_void {
        self.address as *mut c_void
    }
    pub(crate) fn len(self) -> usize {
        self.len
    }
}

pub(crate) trait AcceleratorBackend: Send + Sync {
    fn classify(&self, region: ForeignMemoryRegion) -> StoreResult<PointerMemoryType>;
    fn copy_to_host(&self, destination: &mut [u8], source: ForeignMemoryRegion) -> StoreResult<()>;
    fn copy_from_host(&self, destination: ForeignMemoryRegion, source: &[u8]) -> StoreResult<()>;
}

pub(crate) struct NativeAcceleratorBackend;

pub(crate) fn is_device_memory(
    backend: &dyn AcceleratorBackend,
    region: ForeignMemoryRegion,
) -> StoreResult<bool> {
    Ok(backend.classify(region)? == PointerMemoryType::Device)
}

pub(crate) fn gather_device_to_host(
    backend: &dyn AcceleratorBackend,
    source: ForeignMemoryRegion,
    staging: &mut [u8],
) -> StoreResult<bool> {
    if !is_device_memory(backend, source)? {
        return Ok(false);
    }
    if source.len() > staging.len() {
        return Err(StoreError::InvalidParams(format!(
            "device source size {} exceeds staging buffer size {}",
            source.len(),
            staging.len()
        )));
    }
    backend.copy_to_host(&mut staging[..source.len()], source)?;
    Ok(true)
}

pub(crate) fn scatter_host_to_device(
    backend: &dyn AcceleratorBackend,
    destination: ForeignMemoryRegion,
    staging: &[u8],
) -> StoreResult<()> {
    if staging.len() > destination.len() {
        return Err(StoreError::InvalidParams(format!(
            "staged result size {} exceeds device destination size {}",
            staging.len(),
            destination.len()
        )));
    }
    backend.copy_from_host(destination, staging)
}

impl AcceleratorBackend for NativeAcceleratorBackend {
    fn classify(&self, region: ForeignMemoryRegion) -> StoreResult<PointerMemoryType> {
        // SAFETY: construction of the capability records the raw-API caller's
        // allocation and lifetime guarantee.
        Ok(unsafe { transfer_engine_ffi::classify_pointer(region.as_ptr()) }?)
    }

    fn copy_to_host(&self, destination: &mut [u8], source: ForeignMemoryRegion) -> StoreResult<()> {
        if destination.len() > source.len() {
            return Err(StoreError::InvalidParams(
                "accelerator source region is too small".into(),
            ));
        }
        // SAFETY: the capability and bounds check cover the native read.
        Ok(unsafe { transfer_engine_ffi::copy_to_host(destination, source.as_ptr()) }?)
    }

    fn copy_from_host(&self, destination: ForeignMemoryRegion, source: &[u8]) -> StoreResult<()> {
        if source.len() > destination.len() {
            return Err(StoreError::InvalidParams(
                "accelerator destination region is too small".into(),
            ));
        }
        // SAFETY: the capability and bounds check cover the native write.
        Ok(unsafe { transfer_engine_ffi::copy_from_host(destination.as_mut_ptr(), source) }?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RecordedCopyDirection {
        HostToDevice,
        DeviceToHost,
    }

    #[derive(Debug)]
    struct FakeDevice {
        pointer: usize,
        device_ordinal: i32,
    }

    #[derive(Debug, Default)]
    struct FakeRuntimeState {
        query_count: usize,
        last_reported_device: Option<i32>,
        current_device: Option<i32>,
        last_direction: Option<RecordedCopyDirection>,
    }

    struct FakeRuntimeAccelerator {
        devices: Vec<FakeDevice>,
        state: Mutex<FakeRuntimeState>,
    }

    impl FakeRuntimeAccelerator {
        fn one(pointer: *mut c_void, device_ordinal: i32) -> Self {
            Self {
                devices: vec![FakeDevice {
                    pointer: pointer as usize,
                    device_ordinal,
                }],
                state: Mutex::new(FakeRuntimeState::default()),
            }
        }

        fn matching_device(&self, address: usize) -> Option<i32> {
            self.devices
                .iter()
                .find(|device| device.pointer == address)
                .map(|device| device.device_ordinal)
        }

        fn query_device(&self, address: usize) -> Option<i32> {
            let device = self.matching_device(address);
            let mut state = self.state.lock().unwrap();
            state.query_count += 1;
            state.last_reported_device = device;
            device
        }

        fn select_for_copy(
            &self,
            address: usize,
            direction: RecordedCopyDirection,
            query: bool,
        ) -> StoreResult<i32> {
            let device = if query {
                self.query_device(address)
            } else {
                self.matching_device(address)
            }
            .ok_or_else(|| StoreError::Internal("fake device pointer not found".into()))?;
            let mut state = self.state.lock().unwrap();
            state.current_device = Some(device);
            state.last_direction = Some(direction);
            Ok(device)
        }
    }

    impl AcceleratorBackend for FakeRuntimeAccelerator {
        fn classify(&self, region: ForeignMemoryRegion) -> StoreResult<PointerMemoryType> {
            Ok(if self.query_device(region.address).is_some() {
                PointerMemoryType::Device
            } else {
                PointerMemoryType::Host
            })
        }

        fn copy_to_host(
            &self,
            destination: &mut [u8],
            source: ForeignMemoryRegion,
        ) -> StoreResult<()> {
            self.select_for_copy(source.address, RecordedCopyDirection::DeviceToHost, false)?;
            if destination.len() > source.len() {
                return Err(StoreError::InvalidParams(
                    "fake accelerator source is too small".into(),
                ));
            }
            // SAFETY: each parity test constructs the region from a live array
            // that remains readable for the entire copy.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    source.as_ptr().cast::<u8>(),
                    destination.as_mut_ptr(),
                    destination.len(),
                );
            }
            Ok(())
        }

        fn copy_from_host(
            &self,
            destination: ForeignMemoryRegion,
            source: &[u8],
        ) -> StoreResult<()> {
            self.select_for_copy(
                destination.address,
                RecordedCopyDirection::HostToDevice,
                true,
            )?;
            if source.len() > destination.len() {
                return Err(StoreError::InvalidParams(
                    "fake accelerator destination is too small".into(),
                ));
            }
            // SAFETY: each parity test constructs the region from a live array
            // that remains writable for the entire copy.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    source.as_ptr(),
                    destination.as_mut_ptr().cast::<u8>(),
                    source.len(),
                );
            }
            Ok(())
        }
    }

    struct MockAccelerator {
        memory_type: PointerMemoryType,
        fail_copy: bool,
        copies: Mutex<Vec<&'static str>>,
    }

    impl AcceleratorBackend for MockAccelerator {
        fn classify(&self, _region: ForeignMemoryRegion) -> StoreResult<PointerMemoryType> {
            Ok(self.memory_type)
        }

        fn copy_to_host(
            &self,
            destination: &mut [u8],
            _source: ForeignMemoryRegion,
        ) -> StoreResult<()> {
            if self.fail_copy {
                return Err(StoreError::Internal("mock D2H failure".into()));
            }
            destination.fill(7);
            self.copies.lock().unwrap().push("D2H");
            Ok(())
        }

        fn copy_from_host(
            &self,
            _destination: ForeignMemoryRegion,
            _source: &[u8],
        ) -> StoreResult<()> {
            if self.fail_copy {
                return Err(StoreError::Internal("mock H2D failure".into()));
            }
            self.copies.lock().unwrap().push("H2D");
            Ok(())
        }
    }

    #[test]
    fn foreign_region_records_address_without_dereferencing_it() {
        let mut bytes = [0_u8; 8];
        let region =
            ForeignMemoryRegion::from_caller_owned_raw(bytes.as_mut_ptr().cast(), bytes.len());
        assert_eq!(region.as_ptr(), bytes.as_ptr().cast());
        assert_eq!(region.len(), bytes.len());
    }

    #[test]
    fn host_source_keeps_the_zero_copy_path() {
        let backend = MockAccelerator {
            memory_type: PointerMemoryType::Host,
            fail_copy: false,
            copies: Mutex::new(vec![]),
        };
        let region = ForeignMemoryRegion::from_caller_owned_raw(1_usize as *mut c_void, 4);
        let mut staging = [0_u8; 4];
        assert!(!gather_device_to_host(&backend, region, &mut staging).unwrap());
        assert!(backend.copies.lock().unwrap().is_empty());
    }

    #[test]
    fn device_source_is_gathered_and_copy_failures_propagate() {
        let region = ForeignMemoryRegion::from_caller_owned_raw(1_usize as *mut c_void, 4);
        let backend = MockAccelerator {
            memory_type: PointerMemoryType::Device,
            fail_copy: false,
            copies: Mutex::new(vec![]),
        };
        let mut staging = [0_u8; 4];
        assert!(gather_device_to_host(&backend, region, &mut staging).unwrap());
        assert_eq!(staging, [7; 4]);
        assert_eq!(*backend.copies.lock().unwrap(), ["D2H"]);
        scatter_host_to_device(&backend, region, &staging).unwrap();
        assert_eq!(*backend.copies.lock().unwrap(), ["D2H", "H2D"]);

        let failing = MockAccelerator {
            memory_type: PointerMemoryType::Device,
            fail_copy: true,
            copies: Mutex::new(vec![]),
        };
        assert!(gather_device_to_host(&failing, region, &mut staging).is_err());
        assert!(scatter_host_to_device(&failing, region, &staging).is_err());
    }

    #[test]
    fn device_staging_rejects_insufficient_temporary_space() {
        let backend = MockAccelerator {
            memory_type: PointerMemoryType::Device,
            fail_copy: false,
            copies: Mutex::new(vec![]),
        };
        let region = ForeignMemoryRegion::from_caller_owned_raw(1_usize as *mut c_void, 8);
        assert!(matches!(
            gather_device_to_host(&backend, region, &mut [0_u8; 4]),
            Err(StoreError::InvalidParams(_))
        ));
    }

    #[test]
    fn cpp_parity_runtime_accelerator_test_cpp_runtimeacceleratortest_copyfromhostuseshosttodevicecopy_640ce670()
     {
        let source = *b"abc\0";
        let mut destination = [0_u8; 4];
        let backend = FakeRuntimeAccelerator::one(destination.as_mut_ptr().cast(), 4);
        let region = ForeignMemoryRegion::from_caller_owned_raw(
            destination.as_mut_ptr().cast(),
            destination.len(),
        );

        scatter_host_to_device(&backend, region, &source).unwrap();

        assert_eq!(destination, source);
        let state = backend.state.lock().unwrap();
        assert_eq!(state.query_count, 1);
        assert_eq!(state.last_reported_device, Some(4));
        assert_eq!(state.current_device, Some(4));
        assert_eq!(
            state.last_direction,
            Some(RecordedCopyDirection::HostToDevice)
        );
    }

    #[test]
    fn cpp_parity_runtime_accelerator_test_cpp_runtimeacceleratortest_copytohostusesdevicetohostcopy_df453876()
     {
        let mut source = *b"abc\0";
        let mut destination = [0_u8; 4];
        let backend = FakeRuntimeAccelerator::one(source.as_mut_ptr().cast(), 3);
        let region =
            ForeignMemoryRegion::from_caller_owned_raw(source.as_mut_ptr().cast(), source.len());

        assert!(gather_device_to_host(&backend, region, &mut destination).unwrap());

        assert_eq!(destination, source);
        let state = backend.state.lock().unwrap();
        assert_eq!(state.query_count, 1);
        assert_eq!(state.last_reported_device, Some(3));
        assert_eq!(state.current_device, Some(3));
        assert_eq!(
            state.last_direction,
            Some(RecordedCopyDirection::DeviceToHost)
        );
    }

    #[test]
    fn cpp_parity_runtime_accelerator_test_cpp_runtimeacceleratortest_finddeviceforpointerreturnsmatchingdevice_f7e034ae()
     {
        let mut device_byte = b'd';
        let backend = FakeRuntimeAccelerator::one((&mut device_byte as *mut u8).cast(), 7);
        let region =
            ForeignMemoryRegion::from_caller_owned_raw((&mut device_byte as *mut u8).cast(), 1);

        assert!(is_device_memory(&backend, region).unwrap());

        let state = backend.state.lock().unwrap();
        assert_eq!(state.query_count, 1);
        assert_eq!(state.last_reported_device, Some(7));
        assert_eq!(state.current_device, None);
        assert_eq!(state.last_direction, None);
    }
}
