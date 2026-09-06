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
 */
case class GribIndexEntry(
    messageNumber: Int,
    byteOffset: Long,
    referenceTime: String,
    variableName: String,
    levelDescription: String,
    forecastInfo: String
)

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
      val messageNumber = parts(0).trim.toInt
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
        forecastInfo = forecastInfo
      )
    }.toOption match {
      case Some(entry) => Some(entry)
      case None =>
        logger.debug(s"Skipping malformed idx line: $line")
        None
    }
  }

  /**
   * Compute the (offset, length) byte ranges for each message.
   *
   * Message lengths are inferred from consecutive byte offsets — the length
   * of message N is (offset[N+1] - offset[N]). The last message extends to EOF.
   *
   * @param entries  Parsed index entries (must be in file order)
   * @param fileSize Total size of the GRIB file in bytes
   * @return Sequence of (byteOffset, byteLength) pairs, one per entry
   */
  def computeMessageRanges(entries: Seq[GribIndexEntry], fileSize: Long): Seq[(Long, Long)] = {
    if (entries.isEmpty) return Seq.empty

    entries.zipWithIndex.map { case (entry, i) =>
      val start = entry.byteOffset
      val end = if (i + 1 < entries.length) entries(i + 1).byteOffset else fileSize
      (start, end - start)
    }
  }
}
