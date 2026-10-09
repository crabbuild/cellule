use crate::model::*;
use crate::pipeline::*;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};

fn version(decoder: &mut BoundedDecoder<'_>) -> Result<(), CodecError> {
    if decoder.read_u8()? != 1 {
        return Err(CodecError::Invalid("unsupported release message version"));
    }
    Ok(())
}
fn array<const N: usize>(decoder: &mut BoundedDecoder<'_>) -> Result<[u8; N], CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("release fixed key length differs"))
}
impl WireValue for ReleaseId {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encoder.write_u8(1)?;
        encoder.write_bytes(&self.bytes())
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(decoder)?;
        Self::from_bytes(array(decoder)?)
            .map_err(|_| CodecError::Invalid("invalid release identity"))
    }
}
impl WireValue for TargetName {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encoder.write_u8(1)?;
        encoder.write_text(self.as_str())
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(decoder)?;
        Self::new(decoder.read_text()?.into())
            .map_err(|_| CodecError::Invalid("invalid target name"))
    }
}
impl WireValue for Artifact {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encoder.write_u8(1)?;
        encoder.write_bytes(&self.key)?;
        encoder.write_bytes(&self.digest)?;
        encoder.write_u32(self.bytes)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(decoder)?;
        Ok(Self {
            key: array(decoder)?,
            digest: array(decoder)?,
            bytes: decoder.read_u32()?,
        })
    }
}
// Each nested domain value has its own version. Field order is a persisted
// compatibility contract; changed contracts require an explicit new codec.
macro_rules! structure {
    ($ty:ident {$($field:ident),+ $(,)?}) => {
        impl WireValue for $ty {
            fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
                encoder.write_u8(1)?;
                $(self.$field.encode(encoder)?;)+
                Ok(())
            }
            fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
                version(decoder)?;
                Ok(Self { $($field: WireValue::decode(decoder)?),+ })
            }
        }
    };
}
structure!(Deployment {
    release,
    target,
    artifact,
    expected_generation
});
structure!(Selection { release, artifact });
structure!(TargetState {
    target,
    generation,
    selected
});
structure!(TargetWork { deployment, action });
structure!(TargetRecord {
    deployment,
    outcome,
    installed_generation,
    previous,
    deploys,
    rollbacks
});
impl WireValue for TargetAction {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encoder.write_u8(1)?;
        match self {
            Self::Deploy(bytes) => {
                if bytes.len() > MAX_ARTIFACT_BYTES {
                    return Err(CodecError::Limit);
                }
                encoder.write_u8(0)?;
                encoder.write_bytes(bytes)
            }
            Self::Rollback => encoder.write_u8(1),
        }
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(decoder)?;
        match decoder.read_u8()? {
            0 => {
                let bytes = decoder.read_bytes()?;
                if bytes.len() > MAX_ARTIFACT_BYTES {
                    return Err(CodecError::Limit);
                }
                Ok(Self::Deploy(bytes.to_vec()))
            }
            1 => Ok(Self::Rollback),
            _ => Err(CodecError::Invalid("unknown target action")),
        }
    }
}
impl WireValue for TargetOutcome {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encoder.write_u8(1)?;
        encoder.write_u8(match self {
            Self::Deployed => 0,
            Self::Cancelled => 1,
            Self::RolledBack => 2,
            Self::Superseded => 3,
            Self::Conflict => 4,
            Self::Capacity => 5,
        })
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        version(decoder)?;
        match decoder.read_u8()? {
            0 => Ok(Self::Deployed),
            1 => Ok(Self::Cancelled),
            2 => Ok(Self::RolledBack),
            3 => Ok(Self::Superseded),
            4 => Ok(Self::Conflict),
            5 => Ok(Self::Capacity),
            _ => Err(CodecError::Invalid("unknown target outcome")),
        }
    }
}
macro_rules! field {
    (put,value,$value:expr,$encoder:expr) => {
        $value.encode($encoder)
    };
    (put,fixed,$value:expr,$encoder:expr) => {
        $encoder.write_bytes(&$value)
    };
    (get,value,$decoder:expr) => {
        WireValue::decode($decoder)
    };
    (get,fixed,$decoder:expr) => {
        array($decoder)
    };
}
macro_rules! message {
    ($ty:ident {$($name:ident:$kind:ident),+ $(,)?})=>{
        impl WireValue for $ty {
            fn encode(&self,e:&mut BoundedEncoder)->Result<(),CodecError> {e.write_u8(1)?;$(field!(put,$kind,self.$name,e)?;)+Ok(())}
            fn decode(d:&mut BoundedDecoder<'_>)->Result<Self,CodecError> {version(d)?;Ok(Self {$($name:field!(get,$kind,d)?),+})}
        }
    };
}
message!(ReleaseSpec {
    release: value,
    target: value,
    source: value,
    expected_generation: value,
    target_endpoint: value,
    artifact_endpoint: value,
    approval_deadline_ms: value
});
message!(ArtifactPublication {
    artifact: value,
    etag: fixed
});
message!(Approval {
    release: value,
    input_digest: fixed,
    approve: value
});
message!(Reconcile {
    release: value,
    input_digest: fixed,
    token: value
});
message!(ReleaseRecord {
    release: value,
    target: value,
    input_digest: fixed,
    run_id: fixed,
    definition_version: value,
    revision: value,
    status: value,
    publication: value,
    observed: value,
    verified_generation: value,
    rebuilt: value
});
message!(Projection {
    record: value,
    step: fixed
});
message!(Acknowledgment { projection: value });
macro_rules! tagged {
    ($ty:ident {$($variant:ident=$tag:literal),+ $(,)?})=>{
        impl WireValue for $ty {
            fn encode(&self,e:&mut BoundedEncoder)->Result<(),CodecError> {e.write_u8(1)?;e.write_u8(match self {$(Self::$variant=>$tag),+})}
            fn decode(d:&mut BoundedDecoder<'_>)->Result<Self,CodecError> {version(d)?;match d.read_u8()? {$($tag=>Ok(Self::$variant)),+,_=>Err(CodecError::Invalid("unknown release domain tag"))}}
        }
    };
}
tagged!(ReleaseStatus {AwaitingApproval=0,Active=1,NeedsReview=2,Cancelled=3,RolledBack=4,Superseded=5,Refused=6});
tagged!(ControlOutcome {Accepted=0,Conflict=1,InvalidState=2,Capacity=3});
pub(crate) fn encode_wire<T: WireValue>(value: &T, limit: u32) -> cellule_runtime::Result<Vec<u8>> {
    let mut e = BoundedEncoder::new(limit)?;
    value.encode(&mut e)?;
    Ok(e.finish())
}
pub(crate) fn decode_wire<T: WireValue>(bytes: &[u8], limit: u32) -> cellule_runtime::Result<T> {
    let mut d = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut d)?;
    d.finish()?;
    Ok(value)
}
