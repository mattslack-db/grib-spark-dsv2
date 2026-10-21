package com.example.spark.grib

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.slf4j.LoggerFactory
import java.io.{Closeable, File}
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
  // The `product` struct of currentMessage, built once and shared by all its flat rows.
  private var productRow: InternalRow = _
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
        // Sub-file read: fetch the partition's ranges, then decode lazily.
        readByteRanges(filePath, ranges, computeLatLon)
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
      msg.values(idx).toDouble,                       // value
      ProductAttributes.orNull(msg.pdsTemplate),      // pds_template
      ProductAttributes.orNull(msg.levelValue2),      // level_value2
      productRow                                      // product
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
      valuesArray,                                    // values
      ProductAttributes.orNull(msg.pdsTemplate),      // pds_template
      ProductAttributes.orNull(msg.levelValue2),      // level_value2
      productRow                                      // product
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

  private def setCurrentMessage(msg: DecodedGribMessage): Unit = {
    currentMessage = msg
    productRow = ProductAttributes.toInternalRow(msg.product)
  }

  private def advanceToNextFlatRow(): Unit = {
    // If we have a current message and more points, we're good
    if (currentMessage != null && currentPointIndex < currentMessage.values.length) {
      hasNextRow = true
      return
    }

    // Move to next message
    while (messageIterator.hasNext) {
      setCurrentMessage(messageIterator.next())
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
      setCurrentMessage(messageIterator.next())
      hasNextRow = true
    } else {
      hasNextRow = false
    }
  }

  // ===== File I/O helpers =====

  /**
   * For local paths, return as-is. For any remote filesystem (S3, GCS, ADLS,
   * HDFS, and `dbfs:` — which is how UC volume paths come back from directory
   * listings on Databricks), download to a local temp file, since netCDF-Java
   * needs a local random-access file.
   */
  private def ensureLocalFile(filePath: String): String =
    GribPartitionReader.localPathOf(filePath).getOrElse {
      val path = new Path(filePath)
      val in = FileSystem.get(path.toUri, serializableConf.value).open(path)
      val tempFile = try GribIo.copyToTempFile(in, "grib_") finally in.close()
      tempFiles = tempFiles :+ tempFile
      tempFile.getAbsolutePath
    }

  /**
   * Read a partition's message ranges (index-based partitioning). Ranges that
   * are adjacent in the file are fetched as one sequential read, all over a
   * single open stream, rather than opening the file once per message (on
   * cloud storage, each open is a round trip). See [[SpanMessages]].
   */
  private def readByteRanges(
      filePath: String,
      ranges: Seq[MessageRange],
      computeLatLon: Boolean
  ): Iterator[DecodedGribMessage] = {
    val path = new Path(filePath)
    val in = FileSystem.get(path.toUri, serializableConf.value).open(path)
    val messages = new SpanMessages(in, PartitionPlanning.coalesce(ranges), computeLatLon)
    closeables = messages :: closeables
    messages
  }

  /**
   * The messages of a partition's spans, decoded lazily one span at a time. A
   * span is copied to a local temp file only when it's reached, and its file
   * handle and temp file are released as soon as its messages are used up, so a
   * partition of many scattered spans holds at most one at a time. `close()`
   * releases the current span and the shared input stream.
   */
  private final class SpanMessages(
      in: FSDataInputStream,
      spans: Seq[MessageRange],
      computeLatLon: Boolean
  ) extends Iterator[DecodedGribMessage] with Closeable {

    private val pending = spans.iterator
    private var current: Option[(CloseableIterator[DecodedGribMessage], File)] = None

    override def hasNext: Boolean = {
      while (!current.exists(_._1.hasNext) && pending.hasNext) {
        release()
        open(pending.next())
      }
      val more = current.exists(_._1.hasNext)
      if (!more) release()
      more
    }

    override def next(): DecodedGribMessage =
      if (hasNext) current.get._1.next() else throw new NoSuchElementException("No more GRIB messages")

    override def close(): Unit = {
      release()
      in.close()
    }

    private def open(span: MessageRange): Unit = {
      in.seek(span.offset)
      val file = GribIo.copyToTempFile(in, "grib_range_", span.length)
      try {
        current = Some(GribMessageDecoder.openMessages(file.getAbsolutePath, span.messageIndex, computeLatLon) -> file)
      } catch {
        case e: Throwable =>
          file.delete()
          throw e
      }
    }

    private def release(): Unit = {
      current.foreach { case (messages, file) =>
        try messages.close() finally file.delete()
      }
      current = None
    }
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

object GribPartitionReader {

  /**
   * The local filesystem path for `filePath` if it can be opened directly
   * (no scheme, or `file:`), or None if it lives on a remote filesystem and
   * must be copied locally first.
   */
  def localPathOf(filePath: String): Option[String] = {
    val uri = new Path(filePath).toUri
    Option(uri.getScheme) match {
      case None         => Some(filePath)
      case Some("file") => Some(uri.getPath)
      case Some(_)      => None
    }
  }
}
