use crate::{TransferEngineError, TransferEngineResult, ffi};
use std::ffi::c_char;
use std::mem::MaybeUninit;

#[derive(Debug, Clone, PartialEq)]
pub struct NicLoadStats {
    pub device_name: String,
    pub inflight_bytes: u64,
    pub ewma_bandwidth_bps: f64,
}

pub(crate) fn decode_device_name(raw: &[u8]) -> TransferEngineResult<String> {
    let terminator =
        raw.iter()
            .position(|byte| *byte == 0)
            .ok_or(TransferEngineError::InvalidDeviceName(
                "missing NUL terminator",
            ))?;
    Ok(std::str::from_utf8(&raw[..terminator])?.to_owned())
}

fn c_char_bytes<const N: usize>(raw: &[c_char; N]) -> &[u8] {
    // SAFETY: `c_char` and `u8` are both one byte; this only changes the view.
    unsafe { std::slice::from_raw_parts(raw.as_ptr().cast::<u8>(), N) }
}

pub(crate) fn decode_classic(raw: &ffi::nic_load_stat_t) -> TransferEngineResult<NicLoadStats> {
    Ok(NicLoadStats {
        device_name: decode_device_name(c_char_bytes(&raw.device_name))?,
        inflight_bytes: raw.inflight_bytes,
        ewma_bandwidth_bps: raw.ewma_bandwidth_bps,
    })
}

pub(crate) fn decode_tent(raw: &ffi::tent_nic_load_stat_t) -> TransferEngineResult<NicLoadStats> {
    Ok(NicLoadStats {
        device_name: decode_device_name(c_char_bytes(&raw.device_name))?,
        inflight_bytes: raw.inflight_bytes,
        ewma_bandwidth_bps: raw.ewma_bandwidth_bps,
    })
}

pub(crate) fn query_nic_load_stats<Raw, Call, Decode>(
    operation: &'static str,
    mut call: Call,
    decode: Decode,
) -> TransferEngineResult<Vec<NicLoadStats>>
where
    Call: FnMut(*mut Raw, &mut usize) -> i32,
    Decode: Fn(&Raw) -> TransferEngineResult<NicLoadStats>,
{
    let mut capacity = 0usize;
    for _attempt in 0..8 {
        // The C ABI rejects a null `stats` pointer even for a count-only call.
        let allocation = capacity.max(1);
        let mut raw: Vec<MaybeUninit<Raw>> = Vec::with_capacity(allocation);
        raw.resize_with(allocation, MaybeUninit::uninit);
        let mut count = capacity;
        let code = call(raw.as_mut_ptr().cast::<Raw>(), &mut count);
        if code != 0 {
            return Err(TransferEngineError::NativeOperationFailed { operation, code });
        }
        if count > capacity {
            capacity = count;
            continue;
        }

        let mut stats = Vec::with_capacity(count);
        for item in raw.iter().take(count) {
            // SAFETY: a successful native call promises to initialize `count`
            // records when count does not exceed the advertised capacity.
            stats.push(decode(unsafe { item.assume_init_ref() })?);
        }
        return Ok(stats);
    }
    Err(TransferEngineError::UnstableResultCount(operation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RawStat {
        name: [u8; 8],
        inflight: u64,
        bandwidth: f64,
    }

    fn decode(raw: &RawStat) -> TransferEngineResult<NicLoadStats> {
        Ok(NicLoadStats {
            device_name: decode_device_name(&raw.name)?,
            inflight_bytes: raw.inflight,
            ewma_bandwidth_bps: raw.bandwidth,
        })
    }

    #[test]
    fn retries_when_native_count_grows() {
        let mut calls = 0;
        let stats = query_nic_load_stats(
            "test_stats",
            |raw: *mut RawStat, count| {
                calls += 1;
                if calls == 1 {
                    *count = 2;
                    return 0;
                }
                unsafe {
                    (&mut (*raw).name)[..7].copy_from_slice(b"mlx5_0\0");
                    (*raw).inflight = 17;
                    (*raw).bandwidth = 2.5e9;
                    (&mut (*raw.add(1)).name)[..7].copy_from_slice(b"mlx5_1\0");
                }
                *count = 2;
                0
            },
            decode,
        )
        .unwrap();

        assert_eq!(calls, 2);
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].device_name, "mlx5_0");
        assert_eq!(stats[0].inflight_bytes, 17);
    }

    #[test]
    fn rejects_unterminated_and_invalid_utf8_names() {
        assert!(matches!(
            decode_device_name(b"12345678"),
            Err(TransferEngineError::InvalidDeviceName(_))
        ));
        assert!(matches!(
            decode_device_name(&[0xff, 0]),
            Err(TransferEngineError::InvalidUtf8(_))
        ));
    }

    #[test]
    fn preserves_operation_and_native_error_code() {
        let error = query_nic_load_stats::<RawStat, _, _>(
            "tent_get_nic_load_stats",
            |_raw, _count| -37,
            decode,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TransferEngineError::NativeOperationFailed {
                operation: "tent_get_nic_load_stats",
                code: -37
            }
        ));
    }
}
