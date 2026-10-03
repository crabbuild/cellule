use crate::*;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
pub(crate) fn encode<T: WireValue>(value: &T, limit: u32) -> cellule_runtime::Result<Vec<u8>> {
    let mut e = BoundedEncoder::new(limit)?;
    value.encode(&mut e)?;
    Ok(e.finish())
}
pub(crate) fn decode<T: WireValue>(bytes: &[u8], limit: u32) -> cellule_runtime::Result<T> {
    let mut d = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut d)?;
    d.finish()?;
    Ok(value)
}
fn version(d: &mut BoundedDecoder<'_>) -> Result<(), CodecError> {
    if d.read_u8()? != 1 {
        return Err(CodecError::Invalid("unsupported telemetry wire version"));
    }
    Ok(())
}
fn fixed<const N: usize>(d: &mut BoundedDecoder<'_>) -> Result<[u8; N], CodecError> {
    d.read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("telemetry fixed identity length differs"))
}
fn write_list<T: WireValue>(
    e: &mut BoundedEncoder,
    values: &[T],
    max: usize,
) -> Result<(), CodecError> {
    if values.len() > max {
        return Err(CodecError::Invalid("telemetry list exceeds bound"));
    }
    e.write_count(values.len())?;
    for value in values {
        value.encode(e)?;
    }
    Ok(())
}
fn read_list<T: WireValue>(d: &mut BoundedDecoder<'_>, max: usize) -> Result<Vec<T>, CodecError> {
    let count = d.read_u32()? as usize;
    if count > max {
        return Err(CodecError::Invalid("telemetry list exceeds bound"));
    }
    (0..count).map(|_| T::decode(d)).collect()
}
macro_rules! fields {
 ($ty:ty,$($field:ident:$kind:ty),+ $(,)?)=>{impl WireValue for $ty {
 fn encode(&self,e:&mut BoundedEncoder)->Result<(),CodecError>{e.write_u8(1)?;$(self.$field.encode(e)?;)+Ok(())}
 fn decode(d:&mut BoundedDecoder<'_>)->Result<Self,CodecError>{version(d)?;Ok(Self{$($field:<$kind>::decode(d)?,)+})}
 }};
}
impl WireValue for DeviceKey {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.as_str().to_owned().encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::new(String::decode(d)?)
            .map_err(|_| CodecError::Invalid("noncanonical telemetry device key"))
    }
}
macro_rules! identity {
    ($ty:ty) => {
        impl WireValue for $ty {
            fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
                e.write_bytes(&self.bytes())
            }
            fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
                Self::from_bytes(fixed(d)?)
                    .map_err(|_| CodecError::Invalid("invalid telemetry UUID bytes"))
            }
        }
    };
}
identity!(BatchId);
identity!(MessageId);
fields!(Window,start_ms:i64,minutes:u32);
fields!(Event,device:DeviceKey,sequence:i64,at_ms:i64,value_milli:i64);
fields!(Registration,device:DeviceKey,window:Window);
fields!(RecordedEvent,event:Event,reordered:bool);
fields!(Bucket,start_ms:i64,count:u32,sum_milli:i64);
fields!(DeviceOutcome,decision:Decision,version:Option<Version>);
fields!(EntryResult,event:Event,outcome:DeviceOutcome,source:Option<SourceReceipt>);
fields!(BucketPageRequest,after_ms:Option<i64>,limit:u32);
macro_rules! tagged {
 ($ty:ty,$($tag:literal=>$variant:ident),+)=>{impl WireValue for $ty {fn encode(&self,e:&mut BoundedEncoder)->Result<(),CodecError>{e.write_u8(1)?;e.write_u8(match self{$(Self::$variant=>$tag,)+})}fn decode(d:&mut BoundedDecoder<'_>)->Result<Self,CodecError>{version(d)?;match d.read_u8()?{$($tag=>Ok(Self::$variant),)+_=>Err(CodecError::Invalid("invalid telemetry outcome tag"))}}}};
}
tagged!(Decision,1=>Applied,2=>Duplicate,3=>NotRegistered,4=>Conflict,5=>OutsideWindow,6=>Capacity,7=>Invalid,8=>NotInRoster);
tagged!(ProjectionOutcome,1=>Applied,2=>Duplicate,3=>Stale,4=>Conflict,5=>Capacity);
impl WireValue for Batch {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(1)?;
        self.id.encode(e)?;
        write_list(e, &self.events, MAX_BATCH)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(d)?;
        Ok(Self {
            id: BatchId::decode(d)?,
            events: read_list(d, MAX_BATCH)?,
        })
    }
}
impl WireValue for DeviceState {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(1)?;
        self.device.encode(e)?;
        self.window.encode(e)?;
        self.revision.encode(e)?;
        write_list(e, &self.events, MAX_EVENTS)?;
        e.write_bytes(&self.effect_id)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(d)?;
        Ok(Self {
            device: DeviceKey::decode(d)?,
            window: Window::decode(d)?,
            revision: i64::decode(d)?,
            events: read_list(d, MAX_EVENTS)?,
            effect_id: fixed(d)?,
        })
    }
}
impl WireValue for DeviceSnapshot {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(1)?;
        self.device.encode(e)?;
        self.window.encode(e)?;
        self.revision.encode(e)?;
        self.accepted.encode(e)?;
        self.reordered.encode(e)?;
        self.max_sequence.encode(e)?;
        self.contiguous_sequence.encode(e)?;
        self.latest.encode(e)?;
        write_list(e, &self.buckets, MAX_MINUTES)?;
        e.write_bytes(&self.digest)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(d)?;
        Ok(Self {
            device: DeviceKey::decode(d)?,
            window: Window::decode(d)?,
            revision: i64::decode(d)?,
            accepted: u32::decode(d)?,
            reordered: u32::decode(d)?,
            max_sequence: i64::decode(d)?,
            contiguous_sequence: i64::decode(d)?,
            latest: Option::<Event>::decode(d)?,
            buckets: read_list(d, MAX_MINUTES)?,
            digest: fixed(d)?,
        })
    }
}
impl WireValue for Completion {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(1)?;
        self.message.encode(e)?;
        self.batch.encode(e)?;
        write_list(e, &self.results, MAX_BATCH)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(d)?;
        Ok(Self {
            message: MessageId::decode(d)?,
            batch: Batch::decode(d)?,
            results: read_list(d, MAX_BATCH)?,
        })
    }
}
impl WireValue for AuditOutcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(1)?;
        match self {
            Self::Complete(v) => {
                e.write_u8(1)?;
                v.encode(e)
            }
            Self::Conflict => e.write_u8(2),
            Self::Capacity => e.write_u8(3),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(d)?;
        match d.read_u8()? {
            1 => Ok(Self::Complete(Completion::decode(d)?)),
            2 => Ok(Self::Conflict),
            3 => Ok(Self::Capacity),
            _ => Err(CodecError::Invalid("invalid telemetry audit tag")),
        }
    }
}
impl WireValue for BucketPage {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(1)?;
        self.devices.encode(e)?;
        self.accepted.encode(e)?;
        write_list(e, &self.buckets, 16)?;
        self.next_ms.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(d)?;
        Ok(Self {
            devices: u32::decode(d)?,
            accepted: u32::decode(d)?,
            buckets: read_list(d, 16)?,
            next_ms: Option::<i64>::decode(d)?,
        })
    }
}
impl WireValue for Version {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(1)?;
        self.snapshot.encode(e)?;
        e.write_bytes(&self.effect_id)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(d)?;
        Ok(Self {
            snapshot: DeviceSnapshot::decode(d)?,
            effect_id: fixed(d)?,
        })
    }
}
impl WireValue for SourceReceipt {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(1)?;
        e.write_bytes(&self.cell)?;
        e.write_bytes(&self.incarnation)?;
        self.commit_sequence.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(d)?;
        Ok(Self {
            cell: fixed(d)?,
            incarnation: fixed(d)?,
            commit_sequence: u64::decode(d)?,
        })
    }
}
