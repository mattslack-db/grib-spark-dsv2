//! A planned file's bytes, fetched once and then decoded a chunk of messages
//! at a time (in parallel within a chunk), so a partition's decoded values
//! needn't all be held in memory at once, as the Scala reader streams them.

use std::collections::HashMap;

use bytes::Bytes;
use object_store::ObjectStoreExt;
use rayon::prelude::*;

use crate::decode::{Decoded, DecodedField, decode_all};

/// A decoded message's failures, submessage count and kept fields.
type ChunkResult = (Vec<String>, usize, Vec<DecodedField>);
use crate::error::{Error, Result};
use crate::idx::{IndexEntry, IndexedMessage};
use crate::reader::{FilePlan, ReadStats, resolve};
use crate::selection::Selection;

const GRIB_MAGIC: &[u8; 4] = b"GRIB";
const GRIB2_HEADER_OCTETS: usize = 16;
const GRIB2: u8 = 2;

struct FetchedMessage {
    bytes: Bytes,
    /// The `.idx`-located message's index and fields, or None when the file is read whole.
    indexed: Option<(usize, Vec<IndexEntry>)>,
}

pub struct Scan {
    messages: Vec<FetchedMessage>,
    cursor: usize,
    /// Next field index when numbering a whole file's fields in order.
    next_index: usize,
    selection: Selection,
    with_lat_lons: bool,
    pub stats: ReadStats,
}

impl Scan {
    /// Fetch a planned file's bytes: each planned message's range, or the whole file.
    pub async fn open(plan: &FilePlan, options: &HashMap<String, String>, selection: Selection, with_lat_lons: bool) -> Result<Self> {
        let resolved = resolve(&plan.location, options)?;
        let messages: Vec<FetchedMessage> = match &plan.messages {
            Some(planned) => {
                let ranges: Vec<_> = planned.iter().map(IndexedMessage::range).collect();
                // get_ranges coalesces nearby ranges and issues the requests concurrently.
                let buffers = if ranges.is_empty() { vec![] } else { resolved.store.get_ranges(&resolved.path, &ranges).await? };
                planned
                    .iter()
                    .zip(buffers)
                    .map(|(m, bytes)| FetchedMessage { bytes, indexed: Some((m.message_index, m.entries.clone())) })
                    .collect()
            }
            None => {
                if selection.levels.is_some() {
                    return Err(Error::LevelsNeedIndex);
                }
                let bytes = resolved.store.get(&resolved.path).await?.bytes().await?;
                split_messages(&bytes).into_iter().map(|span| FetchedMessage { bytes: bytes.slice(span), indexed: None }).collect()
            }
        };
        let stats = ReadStats {
            messages_read: messages.len(),
            bytes_read: messages.iter().map(|m| m.bytes.len() as u64).sum(),
            decode_failures: vec![],
        };
        Ok(Self { messages, cursor: 0, next_index: 0, selection, with_lat_lons, stats })
    }

    /// Decode the next `chunk` messages and return the fields kept, or None
    /// once every message has been decoded.
    pub fn next_chunk(&mut self, chunk: usize) -> Option<Result<Vec<DecodedField>>> {
        if self.cursor >= self.messages.len() {
            return None;
        }
        let end = (self.cursor + chunk.max(1)).min(self.messages.len());
        let batch = &self.messages[self.cursor..end];
        self.cursor = end;
        let (selection, with_lat_lons) = (&self.selection, self.with_lat_lons);
        let decoded: Result<Vec<ChunkResult>> = batch
            .par_iter()
            .map(|m| {
                let first_index = m.indexed.as_ref().map_or(0, |(i, _)| *i);
                let Decoded { fields, failures, submessages } = decode_all(&m.bytes, first_index, with_lat_lons)?;
                let entries = m.indexed.as_ref().map(|(_, e)| e.as_slice());
                Ok((failures, submessages, keep(fields, first_index, entries, selection)?))
            })
            .collect();
        let indexed: Vec<bool> = batch.iter().map(|m| m.indexed.is_some()).collect();
        Some(decoded.map(|chunks| self.number(chunks, &indexed)))
    }

    /// Record failures, and number a whole file's fields in file order (failed
    /// fields still take an index, as in the Scala reader).
    fn number(&mut self, chunks: Vec<ChunkResult>, indexed: &[bool]) -> Vec<DecodedField> {
        let mut out = Vec::new();
        for ((failures, submessages, kept), &is_indexed) in chunks.into_iter().zip(indexed) {
            self.stats.decode_failures.extend(failures);
            if !is_indexed {
                let base = self.next_index;
                self.next_index += submessages;
                out.extend(kept.into_iter().map(|mut f| {
                    f.message_index += i32::try_from(base).expect("GRIB counts fit in i32");
                    f
                }));
            } else {
                out.extend(kept);
            }
        }
        out
    }

    /// Decode everything that's left.
    pub fn collect_all(mut self, chunk: usize) -> Result<(Vec<DecodedField>, ReadStats)> {
        let mut fields = Vec::new();
        while let Some(next) = self.next_chunk(chunk) {
            fields.extend(next?);
        }
        Ok((fields, self.stats))
    }
}

/// The decoded fields `selection` keeps, pairing each with its `.idx` entry
/// (the n-th field of an `.idx`-located message is its n-th entry).
fn keep(fields: Vec<DecodedField>, first_index: usize, entries: Option<&[IndexEntry]>, selection: &Selection) -> Result<Vec<DecodedField>> {
    let mut kept = Vec::new();
    for field in fields {
        let position = usize::try_from(field.message_index).expect("non-negative") - first_index;
        if selection.keeps_field(&field, entries.and_then(|e| e.get(position)))? {
            kept.push(field);
        }
    }
    Ok(kept)
}

/// Byte spans of the GRIB2 messages in a file, from each header's total
/// length (octets 9-16). Anything that isn't a GRIB2 header ends the scan,
/// leaving the rest as one span for the decoder to report on.
fn split_messages(bytes: &[u8]) -> Vec<std::ops::Range<usize>> {
    let mut spans = Vec::new();
    let mut start = 0;
    while start < bytes.len() {
        let length = bytes
            .get(start..start + GRIB2_HEADER_OCTETS)
            .filter(|h| h.starts_with(GRIB_MAGIC) && h[7] == GRIB2)
            .map(|h| u64::from_be_bytes(h[8..16].try_into().expect("8 bytes")))
            .and_then(|l| usize::try_from(l).ok())
            .filter(|&l| l >= GRIB2_HEADER_OCTETS)
            .and_then(|l| start.checked_add(l))
            .filter(|&end| end <= bytes.len());
        let Some(end) = length else {
            spans.push(start..bytes.len());
            break;
        };
        spans.push(start..end);
        start = end;
    }
    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_concatenated_grib2_messages_by_header_length() {
        let message = |len: u64| {
            let mut m = b"GRIB\0\0\0\x02".to_vec();
            m.extend(len.to_be_bytes());
            m.resize(usize::try_from(len).unwrap(), 0);
            m
        };
        let bytes = [message(20), message(32)].concat();
        assert_eq!(split_messages(&bytes), [0..20, 20..52]);
        assert_eq!(split_messages(b"not grib"), vec![0..8]);
        let mut huge = b"GRIB\0\0\0\x02".to_vec();
        huge.extend(u64::MAX.to_be_bytes());
        assert_eq!(split_messages(&huge), vec![0..16]);
    }
}
