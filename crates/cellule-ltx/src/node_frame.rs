//! Verified binary envelopes for multiplexed follower replication.

use bytes::Bytes;

use crate::{Limits, LtxError, Result, SegmentInfo};

const MAGIC: &[u8; 4] = b"CNL1";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = 240;
const NODE_SEQUENCE_OFFSET: usize = 32;
const RANGE_VERSION: u16 = 2;
/// Largest supported node-frame envelope, excluding its bounded LTX body.
pub const MAX_NODE_FRAME_HEADER_BYTES: usize = HEADER_BYTES + 8;
const RANGE_HEADER_BYTES: usize = MAX_NODE_FRAME_HEADER_BYTES;

/// Immutable routing and ordering fields authenticated by a node-log frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeFrameScope {
    /// Session of the leader that published the frame.
    pub leader_session: [u8; 16],
    /// Node-log epoch the frame was captured under.
    pub log_epoch: u64,
    /// Position of the frame in the node log.
    pub node_sequence: u64,
    /// Application the frame belongs to.
    pub application: [u8; 16],
    /// Cell the frame belongs to.
    pub cell: [u8; 32],
    /// Incarnation that captured the frame.
    pub incarnation: [u8; 16],
    /// Cell epoch the segment was captured under.
    pub cell_epoch: u64,
    /// Root commit sequence the segment publishes.
    pub commit_sequence: u64,
}

/// One canonical node-log frame whose complete LTX body has been verified.
///
/// Construction is restricted to [`encode_node_frame`], [`encode_node_frame_range`], and
/// [`inspect_node_frame`], so callers cannot attach trusted metadata to an
/// unchecked body.
#[derive(Clone, Debug)]
pub struct VerifiedNodeFrame {
    scope: NodeFrameScope,
    first_commit_sequence: u64,
    segment: SegmentInfo,
    body: Bytes,
    encoded: Bytes,
}

impl VerifiedNodeFrame {
    /// Assigns an ordered node sequence to an already verified frame.
    ///
    /// This changes only the envelope; the verified LTX body and Cell command
    /// range stay identical. Sign the resulting encoded bytes after assignment.
    /// An exclusively owned frame reuses its allocation, allowing verification
    /// before the shipper's sequence-assignment lock.
    pub fn with_node_sequence(self, node_sequence: u64) -> Result<Self> {
        let Self {
            mut scope,
            first_commit_sequence,
            segment,
            body,
            encoded,
        } = self;
        scope.node_sequence = node_sequence;
        validate_scope(scope)?;
        let header_bytes = encoded.len() - body.len();
        drop(body);
        let mut encoded = match encoded.try_into_mut() {
            Ok(encoded) => encoded,
            Err(encoded) => bytes::BytesMut::from(encoded.as_ref()),
        };
        encoded[NODE_SEQUENCE_OFFSET..NODE_SEQUENCE_OFFSET + 8]
            .copy_from_slice(&node_sequence.to_le_bytes());
        let encoded = encoded.freeze();
        Ok(Self {
            scope,
            first_commit_sequence,
            segment,
            body: encoded.slice(header_bytes..),
            encoded,
        })
    }
    /// First logical command covered by this physical SQLite commit.
    ///
    /// Version-one frames cover exactly `scope().commit_sequence`. Version-two
    /// frames authenticate the complete inclusive range in their envelope.
    #[must_use]
    pub const fn first_commit_sequence(&self) -> u64 {
        self.first_commit_sequence
    }
    /// Returns the routing fields the frame was authenticated with.
    #[must_use]
    pub const fn scope(&self) -> NodeFrameScope {
        self.scope
    }

    /// Returns the manifest expectations of the embedded segment.
    #[must_use]
    pub const fn segment(&self) -> &SegmentInfo {
        &self.segment
    }

    /// Returns the verified LTX body.
    #[must_use]
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Returns the encoded frame bytes as received.
    #[must_use]
    pub fn encoded(&self) -> &Bytes {
        &self.encoded
    }

    /// Returns the digest followers use to distinguish an exact retry from a
    /// conflicting duplicate sequence.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        *blake3::hash(&self.encoded).as_bytes()
    }
}

/// Verifies an LTX body and encodes its canonical node-log frame.
pub fn encode_node_frame(
    scope: NodeFrameScope,
    segment: SegmentInfo,
    body: Bytes,
    limits: Limits,
) -> Result<VerifiedNodeFrame> {
    encode_node_frame_range(scope, scope.commit_sequence, segment, body, limits)
}

/// Encodes one physical cut covering a contiguous logical command range.
///
/// The caller must capture every command in one SQLite transaction. Recovery
/// checks range continuity as well as the complete LTX checksum chain. A
/// singleton retains the byte-identical version-one encoding; other ranges
/// require followers advertising node-log protocol version two.
pub fn encode_node_frame_range(
    scope: NodeFrameScope,
    first_commit_sequence: u64,
    segment: SegmentInfo,
    body: Bytes,
    limits: Limits,
) -> Result<VerifiedNodeFrame> {
    validate_scope(scope)?;
    validate_range(first_commit_sequence, scope.commit_sequence)?;
    validate_body(&body, &segment, limits)?;
    let ranged = first_commit_sequence != scope.commit_sequence;
    let header_bytes = if ranged {
        RANGE_HEADER_BYTES
    } else {
        HEADER_BYTES
    };
    let version = if ranged { RANGE_VERSION } else { VERSION };
    let capacity = header_bytes
        .checked_add(body.len())
        .ok_or(LtxError::Limit(crate::LimitKind::NodeFrameBytes))?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&version.to_le_bytes());
    encoded.extend_from_slice(&(header_bytes as u16).to_le_bytes());
    encoded.extend_from_slice(&scope.leader_session);
    push_u64(&mut encoded, scope.log_epoch);
    push_u64(&mut encoded, scope.node_sequence);
    encoded.extend_from_slice(&scope.application);
    encoded.extend_from_slice(&scope.cell);
    encoded.extend_from_slice(&scope.incarnation);
    push_u64(&mut encoded, scope.cell_epoch);
    push_u64(&mut encoded, scope.commit_sequence);
    push_segment(&mut encoded, &segment);
    push_u64(&mut encoded, body.len() as u64);
    encoded.extend_from_slice(&segment.blake3);
    if ranged {
        push_u64(&mut encoded, first_commit_sequence);
    }
    debug_assert_eq!(encoded.len(), header_bytes);
    encoded.extend_from_slice(&body);
    // Scope, range, and the complete body were verified above. Construct the
    // canonical envelope once rather than decompressing the same LTX again.
    let encoded = Bytes::from(encoded);
    Ok(VerifiedNodeFrame {
        scope,
        first_commit_sequence,
        segment,
        body: encoded.slice(header_bytes..),
        encoded,
    })
}

/// Decodes a canonical frame and verifies all declared LTX metadata.
pub fn inspect_node_frame(encoded: Bytes, limits: Limits) -> Result<VerifiedNodeFrame> {
    if encoded.len() < HEADER_BYTES || &encoded[..4] != MAGIC {
        return Err(LtxError::LTXCorrupted);
    }
    let mut cursor = 4;
    let version = take_u16(&encoded, &mut cursor)?;
    let header_bytes = usize::from(take_u16(&encoded, &mut cursor)?);
    if !matches!(
        (version, header_bytes),
        (VERSION, HEADER_BYTES) | (RANGE_VERSION, RANGE_HEADER_BYTES)
    ) {
        return Err(LtxError::LTXCorrupted);
    }
    let scope = NodeFrameScope {
        leader_session: take_array(&encoded, &mut cursor)?,
        log_epoch: take_u64(&encoded, &mut cursor)?,
        node_sequence: take_u64(&encoded, &mut cursor)?,
        application: take_array(&encoded, &mut cursor)?,
        cell: take_array(&encoded, &mut cursor)?,
        incarnation: take_array(&encoded, &mut cursor)?,
        cell_epoch: take_u64(&encoded, &mut cursor)?,
        commit_sequence: take_u64(&encoded, &mut cursor)?,
    };
    let segment = take_segment(&encoded, &mut cursor)?;
    let body_len = take_u64(&encoded, &mut cursor)?;
    let body_digest: [u8; 32] = take_array(&encoded, &mut cursor)?;
    let first_commit_sequence = if version == RANGE_VERSION {
        let first = take_u64(&encoded, &mut cursor)?;
        if first == scope.commit_sequence {
            return Err(LtxError::LTXCorrupted);
        }
        first
    } else {
        scope.commit_sequence
    };
    validate_range(first_commit_sequence, scope.commit_sequence)?;
    if cursor != header_bytes
        || body_len != segment.size_bytes
        || body_len > limits.max_capture_bytes
        || body_len > usize::MAX as u64
        || encoded.len() != header_bytes.saturating_add(body_len as usize)
    {
        return Err(LtxError::Limit(crate::LimitKind::NodeFrameBytes));
    }
    validate_scope(scope)?;
    let body = encoded.slice(header_bytes..);
    if body_digest != segment.blake3 || body_digest != *blake3::hash(&body).as_bytes() {
        return Err(LtxError::ChecksumMismatch);
    }
    validate_body(&body, &segment, limits)?;
    Ok(VerifiedNodeFrame {
        scope,
        first_commit_sequence,
        segment,
        body,
        encoded,
    })
}

fn validate_range(first: u64, last: u64) -> Result<()> {
    if first == 0 || first > last || last > i64::MAX as u64 {
        return Err(LtxError::InvalidState("invalid node frame commit range"));
    }
    Ok(())
}

fn validate_scope(scope: NodeFrameScope) -> Result<()> {
    if scope.leader_session.iter().all(|byte| *byte == 0)
        || scope.application.iter().all(|byte| *byte == 0)
        || scope.cell.iter().all(|byte| *byte == 0)
        || scope.incarnation.iter().all(|byte| *byte == 0)
        || scope.log_epoch == 0
        || scope.node_sequence == 0
        || scope.cell_epoch == 0
        || scope.commit_sequence == 0
    {
        return Err(LtxError::InvalidState("invalid node frame scope"));
    }
    Ok(())
}

fn validate_body(body: &[u8], segment: &SegmentInfo, limits: Limits) -> Result<()> {
    if body.len() as u64 > limits.max_capture_bytes {
        return Err(LtxError::Limit(crate::LimitKind::NodeFrameBody));
    }
    crate::recovery::verify_segment(body, segment, limits)
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_segment(bytes: &mut Vec<u8>, segment: &SegmentInfo) {
    push_u64(bytes, segment.min_txid);
    push_u64(bytes, segment.max_txid);
    bytes.extend_from_slice(&segment.page_size.to_le_bytes());
    bytes.extend_from_slice(&segment.database_pages.to_le_bytes());
    push_u64(bytes, segment.pre_checksum);
    push_u64(bytes, segment.post_checksum);
    push_u64(bytes, segment.size_bytes);
    bytes.extend_from_slice(&segment.blake3);
}

fn take_segment(bytes: &[u8], cursor: &mut usize) -> Result<SegmentInfo> {
    Ok(SegmentInfo {
        min_txid: take_u64(bytes, cursor)?,
        max_txid: take_u64(bytes, cursor)?,
        page_size: take_u32(bytes, cursor)?,
        database_pages: take_u32(bytes, cursor)?,
        pre_checksum: take_u64(bytes, cursor)?,
        post_checksum: take_u64(bytes, cursor)?,
        size_bytes: take_u64(bytes, cursor)?,
        blake3: take_array(bytes, cursor)?,
    })
}

fn take_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    Ok(u16::from_le_bytes(take_array(bytes, cursor)?))
}

fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(take_array(bytes, cursor)?))
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(take_array(bytes, cursor)?))
}

fn take_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N]> {
    let end = cursor.checked_add(N).ok_or(LtxError::LTXCorrupted)?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(LtxError::LTXCorrupted)?
        .try_into()
        .map_err(|_| LtxError::LTXCorrupted)?;
    *cursor = end;
    Ok(value)
}
