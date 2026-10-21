package com.example.spark.grib

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.sources.Filter

/**
 * Describes a unit of work for reading GRIB data.
 *
 * @param filePath      Path to the GRIB file (local or s3a://)
 * @param messageRanges Optional list of message byte ranges for sub-file
 *                      partitioning. If None, the entire file is read.
 * @param pushedFilters Filters pushed down from Spark for runtime evaluation
 * @param outputMode    "flat" or "gridded"
 */
case class GribInputPartition(
    filePath: String,
    messageRanges: Option[Seq[MessageRange]],
    pushedFilters: Array[Filter],
    outputMode: String
) extends InputPartition with Serializable

/**
 * The byte range of one GRIB message within a file.
 *
 * @param messageIndex 0-based position of the message in the file (its
 *                     `message_index`), independent of how the file is split
 *                     into partitions or which messages were filtered out
 * @param offset       Byte offset of the message start
 * @param length       Byte length of the message
 */
case class MessageRange(messageIndex: Int, offset: Long, length: Long)
