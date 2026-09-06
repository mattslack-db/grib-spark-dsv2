package com.example.spark.grib

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import scala.collection.mutable.ArrayBuffer

/**
 * Plans the input partitions for reading GRIB files. Uses .idx sidecar files
 * when available for sub-file partitioning; falls back to whole-file partitions.
 */
class GribScan(
    options: GribOptions,
    readSchema: StructType,
    pushedFilters: Array[Filter]
) extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(getClass)

  // Capture the session's Hadoop configuration on the driver (includes
  // spark.hadoop.* and cloud credentials) and ship it to executors, rather than
  // building a bare `new Configuration()` there that would miss those settings.
  private val serializableConf =
    new SerializableHadoopConf(SparkSession.active.sessionState.newHadoopConf())

  override def readSchema(): StructType = readSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val hadoopConf = serializableConf.value
    val allPartitions = ArrayBuffer.empty[InputPartition]

    for (inputPath <- options.paths) {
      val path = new Path(inputPath)
      val fs = FileSystem.get(path.toUri, hadoopConf)

      // Expand globs
      val files = if (inputPath.contains("*") || inputPath.contains("?") || inputPath.contains("[")) {
        fs.globStatus(path).map(_.getPath)
      } else if (fs.isDirectory(path)) {
        fs.listStatus(path)
          .filter(s => !s.isDirectory)
          .filter(s => {
            val name = s.getPath.getName.toLowerCase
            name.endsWith(".grib") || name.endsWith(".grib2") || name.endsWith(".grb") ||
              name.endsWith(".grb2") || !name.contains(".")  // NOAA files often have no extension
          })
          .map(_.getPath)
      } else {
        Array(path)
      }

      for (file <- files) {
        val filePath = file.toString
        val fileSize = fs.getFileStatus(file).getLen

        if (options.useIndex) {
          val idxPath = new Path(filePath + ".idx")
          if (fs.exists(idxPath)) {
            val partitions = planWithIndex(fs, filePath, idxPath, fileSize)
            allPartitions ++= partitions
          } else {
            // No index — whole file partition
            allPartitions += GribInputPartition(filePath, None, pushedFilters, options.outputMode)
          }
        } else {
          allPartitions += GribInputPartition(filePath, None, pushedFilters, options.outputMode)
        }
      }
    }

    logger.info(s"Planned ${allPartitions.size} partition(s) across ${options.paths.size} input path(s)")
    allPartitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new GribPartitionReaderFactory(options, serializableConf)
  }

  /**
   * Plan partitions using an .idx sidecar file for sub-file partitioning.
   */
  private def planWithIndex(
      fs: FileSystem,
      filePath: String,
      idxPath: Path,
      fileSize: Long
  ): Seq[InputPartition] = {
    // Read index file
    val idxContent = readTextFile(fs, idxPath)
    val allEntries = GribIndexParser.parse(idxContent)

    // Compute byte ranges on the FULL, in-file-order entry list: the length of
    // message N is (offset[N+1] - offset[N]). This MUST happen before any
    // filtering — dropping entries first would make a surviving entry's range
    // span the filtered-out messages that physically follow it (and the last
    // survivor would extend to EOF), re-decoding data that was meant to be
    // skipped. We pair each entry with its range POSITIONALLY, then filter the
    // pairs, so a dropped entry can never be attached to the wrong range (and
    // duplicate `.idx` message numbers, e.g. from concatenated files, are
    // harmless).
    val allRanges = GribIndexParser.computeMessageRanges(allEntries, fileSize)
    val entryRanges = applyIndexFilters(allEntries.zip(allRanges))

    if (entryRanges.isEmpty) {
      logger.info(s"All messages filtered out for $filePath")
      return Seq.empty
    }

    // Group into partitions of messagesPerPartition
    entryRanges
      .grouped(options.messagesPerPartition)
      .map { group =>
        val messageRanges = group.map(_._2)
        GribInputPartition(
          filePath = filePath,
          messageRanges = Some(messageRanges),
          pushedFilters = pushedFilters,
          outputMode = options.outputMode
        )
      }
      .toSeq
  }

  /**
   * Best-effort pruning of `(entry, byteRange)` pairs before decoding, to skip
   * byte ranges that cannot match. This is NOT the authoritative filter — the
   * reader re-evaluates every filter exactly after decoding (see
   * [[GribFilters.passes]]) — so it must never drop a pair the message-level
   * filter would keep.
   *
   * It prunes only on `variable_short_name`, whose `.idx` token equals the
   * emitted value (the NCEP short name, matched case-insensitively). It does
   * NOT prune on level, because the `.idx` level description ("500 mb") uses a
   * different vocabulary from the decoded `level_type` — pruning on it could
   * drop rows that should survive. Level filtering happens after decoding.
   */
  private def applyIndexFilters(
      entryRanges: Seq[(GribIndexEntry, (Long, Long))]
  ): Seq[(GribIndexEntry, (Long, Long))] = {
    // Option-level variable filter (case-insensitive; kept consistent with the
    // message level via GribFilters.matchesAnyVariable).
    val afterOption = entryRanges.filter {
      case (e, _) => GribFilters.matchesAnyVariable(e.variableName, options.variables)
    }

    // Pushed SQL filters — superset pruning on variable_short_name only.
    pushedFilters.foldLeft(afterOption) { (remaining, filter) =>
      remaining.filter { case (e, _) => GribFilters.indexEntryKept(e, filter) }
    }
  }

  /**
   * Read a text file from any Hadoop-compatible filesystem.
   */
  private def readTextFile(fs: FileSystem, path: Path): String = {
    val stream = fs.open(path)
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"))
      val sb = new StringBuilder
      var line = reader.readLine()
      while (line != null) {
        sb.append(line).append('\n')
        line = reader.readLine()
      }
      sb.toString()
    } finally {
      stream.close()
    }
  }
}
