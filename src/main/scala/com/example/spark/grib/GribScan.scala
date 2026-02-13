package com.example.spark.grib

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.conf.Configuration
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

  override def readSchema(): StructType = readSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val hadoopConf = new Configuration()
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
    new GribPartitionReaderFactory(options)
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
    var entries = GribIndexParser.parse(idxContent)

    // Apply pushed filters at the index level
    entries = applyIndexFilters(entries)

    if (entries.isEmpty) {
      logger.info(s"All messages filtered out for $filePath")
      return Seq.empty
    }

    // Compute byte ranges
    val ranges = GribIndexParser.computeMessageRanges(entries, fileSize)
    val entryRanges = entries.zip(ranges)

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
   * Apply pushed filters against .idx entries to skip irrelevant messages
   * before even planning partitions.
   */
  private def applyIndexFilters(entries: Seq[GribIndexEntry]): Seq[GribIndexEntry] = {
    // Also apply the options-level variable filter
    var filtered = GribIndexParser.filterByVariables(entries, options.variables)
    filtered = GribIndexParser.filterByLevelType(filtered, options.levelType)

    // Apply any pushed SQL filters that can be resolved from index metadata
    pushedFilters.foldLeft(filtered) { (remaining, filter) =>
      filter match {
        case EqualTo("variable_short_name", v: String) =>
          remaining.filter(_.variableName == v)
        case In("variable_short_name", values) =>
          val vs = values.map(_.toString).toSet
          remaining.filter(e => vs.contains(e.variableName))
        case EqualTo("level_type", v: String) =>
          remaining.filter(_.levelDescription.toLowerCase.contains(v.toString.toLowerCase))
        case _ => remaining
      }
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
