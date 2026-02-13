package com.example.spark.grib

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import java.io.{File, FileOutputStream}
import java.sql.Timestamp

/**
 * Reads GRIB data for a single partition and emits InternalRow instances.
 *
 * In flat mode: emits one row per (message, grid point) combination.
 * In gridded mode: emits one row per message with values as an array.
 */
class GribPartitionReader(
    partition: GribInputPartition,
    options: GribOptions
) extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)

  // -- State --
  private var messages: Seq[DecodedGribMessage] = Seq.empty
  private var messageIterator: Iterator[DecodedGribMessage] = Iterator.empty
  private var currentMessage: DecodedGribMessage = _
  private var currentPointIndex: Int = 0
  private var currentRow: InternalRow = _
  private var hasNextRow: Boolean = false
  private var initialized: Boolean = false
  private var tempFiles: Seq[File] = Seq.empty

  /**
   * Lazily initialize: open the file, decode messages, set up iteration.
   */
  private def initialize(): Unit = {
    if (initialized) return
    initialized = true

    val filePath = partition.filePath

    partition.messageRanges match {
      case Some(ranges) =>
        // Sub-file read: fetch specific byte ranges and decode each
        messages = readByteRanges(filePath, ranges)
      case None =>
        // Whole-file read
        val localPath = ensureLocalFile(filePath)
        messages = GribMessageDecoder.decodeFile(localPath)
    }

    // Apply pushed variable/level filters at message level
    messages = applyMessageFilters(messages)

    messageIterator = messages.iterator
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
    // Clean up temp files
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

    val valuesArray = new GenericArrayData(msg.values.map(_.toDouble.asInstanceOf[Any]))

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
      val hadoopConf = new Configuration()
      configureHadoop(hadoopConf)
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
      ranges: Seq[(Long, Long)]
  ): Seq[DecodedGribMessage] = {
    val hadoopConf = new Configuration()
    configureHadoop(hadoopConf)
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

      // Decode the range
      val decoded = GribMessageDecoder.decodeFromFile(tempFile.getAbsolutePath, 2, globalIdx)
      allMessages ++= decoded
      globalIdx += decoded.size
    }

    allMessages.result()
  }

  /**
   * Apply pushed filters at the message level (for filters not already handled
   * at the index level, or when no index is available).
   */
  private def applyMessageFilters(msgs: Seq[DecodedGribMessage]): Seq[DecodedGribMessage] = {
    // First, apply option-level filters
    var filtered = if (options.variables.nonEmpty) {
      msgs.filter(m => options.variables.contains(m.variableShortName))
    } else msgs

    filtered = options.levelType match {
      case Some(lt) => filtered.filter(m => m.levelType.toLowerCase.contains(lt.toLowerCase))
      case None     => filtered
    }

    // Apply pushed SQL filters
    partition.pushedFilters.foldLeft(filtered) { (remaining, filter) =>
      applyFilterToMessages(remaining, filter)
    }
  }

  private def applyFilterToMessages(
      msgs: Seq[DecodedGribMessage],
      filter: Filter
  ): Seq[DecodedGribMessage] = {
    filter match {
      case EqualTo("variable_short_name", v: String) =>
        msgs.filter(_.variableShortName == v)
      case In("variable_short_name", values) =>
        val vs = values.map(_.toString).toSet
        msgs.filter(m => vs.contains(m.variableShortName))
      case EqualTo("level_type", v: String) =>
        msgs.filter(_.levelType.toLowerCase.contains(v.toLowerCase))
      case EqualTo("discipline", v: Int) =>
        msgs.filter(_.discipline == v)
      case GreaterThan("level_value", v: Number) =>
        msgs.filter(_.levelValue > v.doubleValue())
      case GreaterThanOrEqual("level_value", v: Number) =>
        msgs.filter(_.levelValue >= v.doubleValue())
      case LessThan("level_value", v: Number) =>
        msgs.filter(_.levelValue < v.doubleValue())
      case LessThanOrEqual("level_value", v: Number) =>
        msgs.filter(_.levelValue <= v.doubleValue())
      case And(left, right) =>
        applyFilterToMessages(applyFilterToMessages(msgs, left), right)
      case Or(left, right) =>
        val l = applyFilterToMessages(msgs, left).toSet
        val r = applyFilterToMessages(msgs, right).toSet
        (l ++ r).toSeq
      case Not(child) =>
        val excluded = applyFilterToMessages(msgs, child).toSet
        msgs.filterNot(excluded.contains)
      case _ => msgs // Unknown filter — don't filter
    }
  }

  /**
   * Configure Hadoop for common cloud storage access.
   */
  private def configureHadoop(conf: Configuration): Unit = {
    // S3A anonymous access for public buckets (can be overridden by cluster config)
    if (partition.filePath.startsWith("s3a://") || partition.filePath.startsWith("s3://")) {
      if (conf.get("fs.s3a.aws.credentials.provider") == null) {
        // Don't override if already configured
      }
    }
  }

  /**
   * Convert a java.sql.Timestamp to microseconds since epoch (Spark's internal timestamp format).
   */
  private def timestampToMicros(ts: Timestamp): Long = {
    if (ts == null) 0L
    else ts.getTime * 1000L  // milliseconds to microseconds
  }
}
