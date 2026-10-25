//! Parser for wgrib2-style `.idx` sidecar files, ported from `GribIndexParser.scala`.
//!
//! Example line: `2:83345:d=2024010100:TMP:1 mb:anl:`

/// A single entry parsed from a `.idx` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    /// Ordinal message number (1-based).
    pub message_number: u32,
    /// For a GRIB message packing several fields, this field's 1-based position
    /// within it (`.idx` number `12.2` is message 12, sub-message 2).
    pub sub_message: Option<u32>,
    pub byte_offset: u64,
    pub reference_time: String,
    pub variable_name: String,
    pub level_description: String,
    pub forecast_info: String,
}

/// One physical GRIB message located by the index: its fields (one entry, or
/// several sub-messages sharing a byte offset) and its byte range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedMessage {
    /// Position of the message's first field among all entries, so fields are
    /// numbered exactly as a whole-file read numbers them.
    pub message_index: usize,
    pub offset: u64,
    pub length: u64,
    pub entries: Vec<IndexEntry>,
}

impl IndexedMessage {
    pub fn range(&self) -> std::ops::Range<u64> {
        self.offset..self.offset + self.length
    }
}

/// Parse an `.idx` file, or `None` if any non-blank line is malformed. Byte
/// ranges come from consecutive offsets, so a skipped line would silently merge
/// its message into the previous entry's range.
pub fn parse_strict(content: &str) -> Option<Vec<IndexEntry>> {
    content.lines().map(str::trim).filter(|line| !line.is_empty()).map(parse_line).collect()
}

/// Parse an `.idx` file, skipping malformed lines.
pub fn parse(content: &str) -> Vec<IndexEntry> {
    content.lines().map(str::trim).filter(|line| !line.is_empty()).filter_map(parse_line).collect()
}

const MIN_FIELDS: usize = 6;

fn parse_line(line: &str) -> Option<IndexEntry> {
    let parts: Vec<&str> = line.split(':').collect();
    if parts.len() < MIN_FIELDS {
        return None;
    }
    let (message_number, sub_message) = match parts[0].trim().split_once('.') {
        None => (parts[0].trim().parse().ok()?, None),
        Some((number, sub)) => (number.parse().ok()?, Some(sub.parse().ok()?)),
    };
    let ref_time = parts[2].trim();
    Some(IndexEntry {
        message_number,
        sub_message,
        byte_offset: parts[1].trim().parse().ok()?,
        reference_time: ref_time.strip_prefix("d=").unwrap_or(ref_time).to_string(),
        variable_name: parts[3].trim().to_string(),
        level_description: parts[4].trim().to_string(),
        forecast_info: parts[5].trim().to_string(),
    })
}

/// Group entries (in file order) into physical GRIB messages. A message runs
/// from its offset to the next larger offset, or to `file_size` for the last.
/// None if the offsets don't ascend within the file (e.g. a stale `.idx`),
/// since byte ranges can't then be derived from them.
pub fn group_messages(entries: &[IndexEntry], file_size: u64) -> Option<Vec<IndexedMessage>> {
    let ascending = entries.windows(2).all(|w| w[0].byte_offset <= w[1].byte_offset);
    if !ascending || entries.last().is_some_and(|e| e.byte_offset >= file_size) {
        return None;
    }
    let starts: Vec<usize> = (0..entries.len()).filter(|&i| i == 0 || entries[i].byte_offset != entries[i - 1].byte_offset).collect();
    let messages = starts
        .iter()
        .enumerate()
        .map(|(n, &start)| {
            let end = starts.get(n + 1).copied().unwrap_or(entries.len());
            let offset = entries[start].byte_offset;
            let next_offset = entries.get(end).map_or(file_size, |e| e.byte_offset);
            IndexedMessage { message_index: start, offset, length: next_offset - offset, entries: entries[start..end].to_vec() }
        })
        .collect();
    Some(messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "1:0:d=2024010100:HGT:1 mb:anl:
2:83345:d=2024010100:TMP:1 mb:anl:
3:152498:d=2024010100:RH:1 mb:anl:
4:201234:d=2024010100:UGRD:1 mb:anl:
5:270000:d=2024010100:VGRD:1 mb:anl:";

    // UGRD and VGRD packed into one GRIB message: two lines, one byte offset.
    const SUB_MESSAGES: &str = "1:0:d=2024010100:HGT:100 mb:anl:
2:5000:d=2024010100:TMP:100 mb:anl:
3.1:9000:d=2024010100:UGRD:100 mb:anl:
3.2:9000:d=2024010100:VGRD:100 mb:anl:
4:15000:d=2024010100:HGT:125 mb:anl:";

    #[test]
    fn parses_all_fields_of_a_line() {
        let entries = parse(SAMPLE);
        assert_eq!(entries.len(), 5);
        assert_eq!(
            entries[0],
            IndexEntry {
                message_number: 1,
                sub_message: None,
                byte_offset: 0,
                reference_time: "2024010100".into(),
                variable_name: "HGT".into(),
                level_description: "1 mb".into(),
                forecast_info: "anl".into(),
            }
        );
        assert_eq!(entries[1].byte_offset, 83345);
        assert_eq!(entries[1].variable_name, "TMP");
    }

    #[test]
    fn empty_input_yields_no_entries() {
        assert!(parse("").is_empty());
        assert_eq!(parse_strict("  \n"), Some(vec![]));
    }

    #[test]
    fn lenient_parse_skips_malformed_lines_but_strict_rejects_them() {
        let input = "1:0:d=2024010100:HGT:1 mb:anl:\nthis is garbage\n3:152498:d=2024010100:RH:1 mb:anl:";
        let names: Vec<_> = parse(input).into_iter().map(|e| e.variable_name).collect();
        assert_eq!(names, ["HGT", "RH"]);
        assert_eq!(parse_strict(input), None);
    }

    #[test]
    fn tolerates_extra_colon_separated_fields() {
        let entries = parse("1:0:d=2024010100:HGT:1 mb:anl:extra:fields:");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].variable_name, "HGT");
    }

    #[test]
    fn last_message_extends_to_end_of_file() {
        let messages = group_messages(&parse(SAMPLE), 350_000).unwrap();
        let ranges: Vec<_> = messages.iter().map(|m| (m.offset, m.length)).collect();
        assert_eq!(ranges[0], (0, 83345));
        assert_eq!(ranges[1], (83345, 152498 - 83345));
        assert_eq!(ranges[4], (270000, 350000 - 270000));
    }

    #[test]
    fn sub_messages_share_one_physical_message_numbered_by_field_position() {
        let entries = parse_strict(SUB_MESSAGES).unwrap();
        assert_eq!(entries[2].sub_message, Some(1));
        assert_eq!(entries[3].sub_message, Some(2));

        let messages = group_messages(&entries, 20_000).unwrap();
        let ranges: Vec<_> = messages.iter().map(|m| (m.message_index, m.offset, m.length)).collect();
        assert_eq!(ranges, [(0, 0, 5000), (1, 5000, 4000), (2, 9000, 6000), (4, 15000, 5000)]);
        let names: Vec<_> = messages[2].entries.iter().map(|e| e.variable_name.as_str()).collect();
        assert_eq!(names, ["UGRD", "VGRD"]);
    }

    #[test]
    fn no_entries_yield_no_messages() {
        assert_eq!(group_messages(&[], 100), Some(vec![]));
    }

    #[test]
    fn offsets_that_do_not_ascend_within_the_file_are_rejected() {
        let descending = parse("1:500:d=2024010100:HGT:1 mb:anl:\n2:100:d=2024010100:TMP:1 mb:anl:");
        assert_eq!(group_messages(&descending, 1000), None);
        assert_eq!(group_messages(&parse(SAMPLE), 270_000), None, "stale index past the end of the file");
    }
}
