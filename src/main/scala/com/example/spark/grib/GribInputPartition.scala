package com.example.spark.grib

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.sources.Filter

/**
 * Describes a unit of work for reading GRIB data.
 *
 * @param filePath      Path to the GRIB file (local or s3a://)
 * @param messageRanges Optional list of (byteOffset, byteLength) pairs for
 *                      sub-file partitioning. If None, the entire file is read.
 * @param pushedFilters Filters pushed down from Spark for runtime evaluation
 * @param outputMode    "flat" or "gridded"
 */
case class GribInputPartition(
    filePath: String,
    messageRanges: Option[Seq[(Long, Long)]],
    pushedFilters: Array[Filter],
    outputMode: String
) extends InputPartition with Serializable
