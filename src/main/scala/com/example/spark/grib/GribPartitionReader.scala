package com.example.spark.grib

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import java.io.{Closeable, File, FileOutputStream}
import java.sql.Timestamp

/**
 * Reads GRIB data for a single partition and emits InternalRow instances.
 *
 * In flat mode: emits one row per (message, grid point) combination.
 * In gridded mode: emits one row per message with values as an array.
 */
class GribPartitionReader(
    partition: GribInputPartition,
    options: GribOptions,
    serializableConf: SerializableHadoopConf
) extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)

  // -- State --
  private var messageIterator: Iterator[DecodedGribMessage] = Iterator.empty
  private var currentMessage: DecodedGribMessage = _
  private var currentPointIndex: Int = 0
  private var currentRow: InternalRow = _
  private var hasNextRow: Boolean = false
  private var initialized: Boolean = false
  private var tempFiles: Seq[File] = Seq.empty
  // Open file handles (streaming iterators) to release in close().
  private var closeables: List[Closeable] = Nil

  /**
   * Lazily initialize: open the file, set up (lazy) message iteration.
   */
  private def initialize(): Unit = {
    if (initialized) return
    initialized = true

    val filePath = partition.filePath
    // Per-point lat/lon arrays are only needed in flat mode.
    val computeLatLon = options.outputMode == GribOptions.OUTPUT_MODE_FLAT

    val rawMessages: Iterator[DecodedGribMessage] = partition.messageRanges match {
      case Some(ranges) =>
        // Sub-file read: bounded by messagesPerPartition, decoded eagerly.
        readByteRanges(filePath, ranges, computeLatLon).iterator
      case None =>
        // Whole-file read: stream messages lazily so the whole file's decoded
        // values are never held in memory at once.
        val localPath = ensureLocalFile(filePath)
        val src = GribMessageDecoder.openMessages(localPath, computeLatLon = computeLatLon)
        closeables = src :: closeables
        src
    }

    // Apply option-level and pushed filters lazily, one message at a time.
    messageIterator = rawMessages.filter(messagePasses)
    advanceToNextRow()
  }

  override def next(): Boolean = {
    initialize()
    val result = hasNextRow
    if (result) {
      // Build the current row, then advance
      currentRow = buildCurrentRow()
      advanceAfterEmit()
    }
    result
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    // Release streaming file handles first (they may hold the temp files open),
    // then delete temp files. Runs even if iteration ended early.
    closeables.foreach { c =>
      try { c.close() } catch { case _: Exception => }
    }
    closeables = Nil

    tempFiles.foreach { f =>
      try { f.delete() } catch { case _: Exception => }
    }
    tempFiles = Seq.empty
  }

  // ===== Row construction =====

  private def buildCurrentRow(): InternalRow = {
    options.outputMode match {
      case GribOptions.OUTPUT_MODE_FLAT    => buildFlatRow()
      case GribOptions.OUTPUT_MODE_GRIDDED => buildGriddedRow()
    }
  }

  /**
   * Build a flat-mode row: one row per grid point.
   */
  private def buildFlatRow(): InternalRow = {
    val msg = currentMessage
    val idx = currentPointIndex

    var lon = msg.lons(idx)
    if (options.longitudeRange == GribOptions.LONGITUDE_NEG180 && lon > 180.0) {
      lon -= 360.0
    }

    InternalRow(
      UTF8String.fromString(partition.filePath),     // file_path
      msg.messageIndex,                               // message_index
      msg.discipline,                                 // discipline
      UTF8String.fromString(msg.variableShortName),  // variable_short_name
      UTF8String.fromString(msg.variableName),       // variable_name
      UTF8String.fromString(msg.units),              // units
      UTF8String.fromString(msg.levelType),          // level_type
      msg.levelValue,                                 // level_value
      timestampToMicros(msg.referenceTime),           // reference_time
      timestampToMicros(msg.forecastTime),            // forecast_time
      msg.lats(idx),                                  // latitude
      lon,                                            // longitude
      msg.values(idx).toDouble                        // value
    )
  }

  /**
   * Build a gridded-mode row: one row per message with values as array.
   */
  private def buildGriddedRow(): InternalRow = {
    val msg = currentMessage

    var lonStart = msg.lonStart
    if (options.longitudeRange == GribOptions.LONGITUDE_NEG180 && lonStart > 180.0) {
      lonStart -= 360.0
    }

    // Build a primitive Array[Double] so GenericArrayData's primitive
    // constructor is used, avoiding boxing every one of the nx*ny values.
    val valuesArray = new GenericArrayData(msg.values.map(_.toDouble))

    InternalRow(
      UTF8String.fromString(partition.filePath),     // file_path
      msg.messageIndex,                               // message_index
      msg.discipline,                                 // discipline
      UTF8String.fromString(msg.variableShortName),  // variable_short_name
      UTF8String.fromString(msg.variableName),       // variable_name
      UTF8String.fromString(msg.units),              // units
      UTF8String.fromString(msg.levelType),          // level_type
      msg.levelValue,                                 // level_value
      timestampToMicros(msg.referenceTime),           // reference_time
      timestampToMicros(msg.forecastTime),            // forecast_time
      msg.nx,                                         // nx
      msg.ny,                                         // ny
      msg.latStart,                                   // lat_start
      lonStart,                                       // lon_start
      msg.latIncrement,                               // lat_increment
      msg.lonIncrement,                               // lon_increment
      valuesArray                                     // values
    )
  }

  // ===== Iteration control =====

  /**
   * Advance internal state to the next row that will be emitted.
   */
  private def advanceToNextRow(): Unit = {
    options.outputMode match {
      case GribOptions.OUTPUT_MODE_FLAT    => advanceToNextFlatRow()
      case GribOptions.OUTPUT_MODE_GRIDDED => advanceToNextGriddedRow()
    }
  }

  /**
   * After emitting a row, advance to the next position.
   */
  private def advanceAfterEmit(): Unit = {
    options.outputMode match {
      case GribOptions.OUTPUT_MODE_FLAT =>
        currentPointIndex += 1
        advanceToNextFlatRow()
      case GribOptions.OUTPUT_MODE_GRIDDED =>
        advanceToNextGriddedRow()
    }
  }

  private def advanceToNextFlatRow(): Unit = {
    // If we have a current message and more points, we're good
    if (currentMessage != null && currentPointIndex < currentMessage.values.length) {
      hasNextRow = true
      return
    }

    // Move to next message
    while (messageIterator.hasNext) {
      currentMessage = messageIterator.next()
      currentPointIndex = 0
      if (currentMessage.values.nonEmpty) {
        hasNextRow = true
        return
      }
    }

    hasNextRow = false
  }

  private def advanceToNextGriddedRow(): Unit = {
    if (messageIterator.hasNext) {
      currentMessage = messageIterator.next()
      hasNextRow = true
    } else {
      hasNextRow = false
    }
  }

  // ===== File I/O helpers =====

  /**
   * For S3/remote paths, download to a local temp file.
   * For local paths, return as-is.
   */
  private def ensureLocalFile(filePath: String): String = {
    if (filePath.startsWith("s3a://") || filePath.startsWith("s3://") ||
        filePath.startsWith("gs://") || filePath.startsWith("hdfs://") ||
        filePath.startsWith("abfss://")) {
      val hadoopConf = serializableConf.value
      val path = new Path(filePath)
      val fs = FileSystem.get(path.toUri, hadoopConf)
      val tempFile = File.createTempFile("grib_", ".tmp")
      tempFile.deleteOnExit()
      tempFiles = tempFiles :+ tempFile

      val in = fs.open(path)
      val out = new FileOutputStream(tempFile)
      try {
        val buf = new Array[Byte](65536)
        var n = in.read(buf)
        while (n > 0) {
          out.write(buf, 0, n)
          n = in.read(buf)
        }
      } finally {
        in.close()
        out.close()
      }
      tempFile.getAbsolutePath
    } else {
      // Handle file:// URIs
      if (filePath.startsWith("file:")) {
        new java.net.URI(filePath).getPath
      } else {
        filePath
      }
    }
  }

  /**
   * Read specific byte ranges from a file (for index-based partitioning).
   */
  private def readByteRanges(
      filePath: String,
      ranges: Seq[(Long, Long)],
      computeLatLon: Boolean
  ): Seq[DecodedGribMessage] = {
    val hadoopConf = serializableConf.value
    val path = new Path(filePath)
    val fs = FileSystem.get(path.toUri, hadoopConf)

    val allMessages = Seq.newBuilder[DecodedGribMessage]
    var globalIdx = 0

    for ((offset, length) <- ranges) {
      val tempFile = File.createTempFile("grib_range_", ".tmp")
      tempFile.deleteOnExit()
      tempFiles = tempFiles :+ tempFile

      // Read the byte range
      val in = fs.open(path)
      val out = new FileOutputStream(tempFile)
      try {
        in.seek(offset)
        val buf = new Array[Byte](Math.min(65536, length.toInt))
        var remaining = length
        while (remaining > 0) {
          val toRead = Math.min(buf.length.toLong, remaining).toInt
          val n = in.read(buf, 0, toRead)
          if (n <= 0) {
            remaining = 0
          } else {
            out.write(buf, 0, n)
            remaining -= n
          }
        }
      } finally {
        in.close()
        out.close()
      }

      // Decode the range. Edition is auto-detected per slice, so GRIB1 files
      // with an .idx sidecar decode correctly too (each slice begins on a
      // message boundary, i.e. the "GRIB" magic).
      val decoded = GribMessageDecoder.decodeFile(tempFile.getAbsolutePath, globalIdx, computeLatLon)
      allMessages ++= decoded
      globalIdx += decoded.size
    }

    allMessages.result()
  }

  /**
   * The authoritative message-level filter, evaluated one message at a time.
   * Pushed SQL predicates are evaluated exactly here (see [[GribFilters.passes]]),
   * so this must fully honour every filter that was pushed down.
   */
  private def messagePasses(m: DecodedGribMessage): Boolean = {
    // Option-level filters (user convenience; matched leniently by design).
    val optionOk =
      GribFilters.matchesAnyVariable(m.variableShortName, options.variables) &&
        (options.levelType match {
          case Some(lt) => m.levelType.toLowerCase.contains(lt.toLowerCase)
          case None     => true
        })

    // Pushed SQL filters — exact evaluation. Every pushed filter must pass.
    optionOk && partition.pushedFilters.forall(f => GribFilters.passes(m, f))
  }

  /**
   * Convert a java.sql.Timestamp to microseconds since epoch (Spark's internal timestamp format).
   */
  private def timestampToMicros(ts: Timestamp): Long = {
    if (ts == null) 0L
    else ts.getTime * 1000L  // milliseconds to microseconds
  }
}
