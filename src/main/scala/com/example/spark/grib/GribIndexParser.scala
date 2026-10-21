package com.example.spark.grib

import org.slf4j.LoggerFactory
import scala.util.Try

/**
 * A single entry parsed from a GRIB .idx sidecar file.
 *
 * Example .idx line: `2:83345:d=2024010100:TMP:1 mb:anl:`
 *
 * @param messageNumber  Ordinal message number (1-based)
 * @param byteOffset     Byte offset of the message start in the GRIB file
 * @param referenceTime  Reference time string (e.g. "2024010100")
 * @param variableName   Variable short name (e.g. "TMP")
 * @param levelDescription Level description string (e.g. "1 mb", "surface")
 * @param forecastInfo   Forecast info string (e.g. "anl", "6 hour fcst")
 * @param subMessage     For a GRIB message packing several fields, this field's
 *                       1-based position within it (`.idx` number `12.2` is
 *                       message 12, sub-message 2). All of a message's fields
 *                       share its byte offset.
 */
case class GribIndexEntry(
    messageNumber: Int,
    byteOffset: Long,
    referenceTime: String,
    variableName: String,
    levelDescription: String,
    forecastInfo: String,
    subMessage: Option[Int] = None
)

/**
 * One physical GRIB message located by the `.idx` file: its fields (one entry,
 * or several sub-messages) and its byte range. `range.messageIndex` is the
 * position of its first field, so decoding it numbers the fields exactly as a
 * whole-file read does.
 */
case class IndexedMessage(entries: Seq[GribIndexEntry], range: MessageRange)

/**
 * Parser for GRIB .idx sidecar index files.
 *
 * These index files are commonly produced by wgrib/wgrib2 and are distributed
 * alongside GRIB files from NOAA and other providers. They enable efficient
 * random access to individual messages within a GRIB file via byte offsets.
 */
object GribIndexParser {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Parse the contents of an .idx file into a sequence of index entries.
   *
   * Format: `messageNumber:byteOffset:d=YYYYMMDDHH:variable:level:forecastInfo:`
   * Some variations may include additional colon-separated fields.
   *
   * @param idxContent The full text content of the .idx file
   * @return Parsed entries, skipping malformed lines
   */
  def parse(idxContent: String): Seq[GribIndexEntry] = {
    if (idxContent == null || idxContent.trim.isEmpty) return Seq.empty

    idxContent.linesIterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap(parseLine)
      .toSeq
  }

  /**
   * Parse an .idx file, or None if any non-blank line is malformed. Byte ranges
   * come from consecutive offsets, so a skipped line would silently merge its
   * message into the previous entry's range: an index that doesn't fully parse
   * can't be trusted for partitioning or pruning.
   */
  def parseStrict(idxContent: String): Option[Seq[GribIndexEntry]] = {
    val lines = Option(idxContent).getOrElse("").linesIterator.map(_.trim).filter(_.nonEmpty).toVector
    val entries = lines.flatMap(parseLine)
    if (entries.length == lines.length) Some(entries) else None
  }

  /**
   * Parse a single .idx line.
   *
   * @param line A single line from the .idx file
   * @return Some(entry) if parseable, None otherwise
   */
  private def parseLine(line: String): Option[GribIndexEntry] = {
    // Split on ':' — minimum 6 fields
    val parts = line.split(":", -1) // -1 to keep trailing empty strings
    if (parts.length < 6) {
      logger.debug(s"Skipping malformed idx line (too few fields): $line")
      return None
    }

    Try {
      val (messageNumber, subMessage) = parts(0).trim.split('.') match {
        case Array(number)      => (number.toInt, None)
        case Array(number, sub) => (number.toInt, Some(sub.toInt))
        case _                  => throw new NumberFormatException(s"Bad message number: ${parts(0)}")
      }
      val byteOffset = parts(1).trim.toLong
      val refTimeRaw = parts(2).trim
      val referenceTime = if (refTimeRaw.startsWith("d=")) refTimeRaw.substring(2) else refTimeRaw
      val variableName = parts(3).trim
      val levelDescription = parts(4).trim
      val forecastInfo = parts(5).trim

      GribIndexEntry(
        messageNumber = messageNumber,
        byteOffset = byteOffset,
        referenceTime = referenceTime,
        variableName = variableName,
        levelDescription = levelDescription,
        forecastInfo = forecastInfo,
        subMessage = subMessage
      )
    }.toOption match {
      case Some(entry) => Some(entry)
      case None =>
        logger.debug(s"Skipping malformed idx line: $line")
        None
    }
  }

  /**
   * Compute the (offset, length) byte range of each entry's GRIB message.
   *
   * A message runs from its offset to the next larger offset (or EOF for the
   * last one). Sub-messages share their message's offset, so each gets the
   * whole message's range.
   *
   * @param entries  Parsed index entries (must be in file order)
   * @param fileSize Total size of the GRIB file in bytes
   * @return Sequence of (byteOffset, byteLength) pairs, one per entry
   */
  def computeMessageRanges(entries: Seq[GribIndexEntry], fileSize: Long): Seq[(Long, Long)] = {
    // Walk backwards so each entry sees the next offset beyond its own.
    val ends = entries.map(_.byteOffset).scanRight((fileSize, fileSize)) {
      case (offset, (nextOffset, nextEnd)) => (offset, if (offset == nextOffset) nextEnd else nextOffset)
    }.init.map(_._2)
    entries.zip(ends).map { case (entry, end) => (entry.byteOffset, end - entry.byteOffset) }
  }

  /**
   * Group entries into physical GRIB messages (sub-messages sharing a byte
   * offset form one message), each with its byte range and file-level
   * message_index: the position of its first field among all entries.
   *
   * @param entries  Parsed index entries (must be in file order)
   * @param fileSize Total size of the GRIB file in bytes
   */
  def groupMessages(entries: Seq[GribIndexEntry], fileSize: Long): Seq[IndexedMessage] = {
    val ranges = computeMessageRanges(entries, fileSize)
    val positioned = entries.zip(ranges).zipWithIndex

    positioned.foldRight(List.empty[IndexedMessage]) {
      case (((entry, (offset, length)), position), next :: rest) if next.range.offset == offset =>
        IndexedMessage(entry +: next.entries, MessageRange(position, offset, length)) :: rest
      case (((entry, (offset, length)), position), acc) =>
        IndexedMessage(Seq(entry), MessageRange(position, offset, length)) :: acc
    }
  }
}
