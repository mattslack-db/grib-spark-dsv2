package com.example.spark.grib

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._

/**
 * Parsed and validated options for the GRIB data source.
 *
 * @param paths              Input file/directory paths
 * @param variables          Optional filter: only read these variable short names
 * @param levelType          Optional filter: only read messages matching this level type
 * @param outputMode         "flat" (one row per grid point) or "gridded" (one row per message)
 * @param messagesPerPartition Number of GRIB messages per Spark partition when using index
 * @param longitudeRange     "0to360" or "-180to180"
 * @param useIndex           Whether to look for and use .idx sidecar files
 */
case class GribOptions(
    paths: Seq[String],
    variables: Set[String],
    levelType: Option[String],
    outputMode: String,
    messagesPerPartition: Int,
    longitudeRange: String,
    useIndex: Boolean
)

object GribOptions {

  val OUTPUT_MODE_FLAT    = "flat"
  val OUTPUT_MODE_GRIDDED = "gridded"

  val LONGITUDE_0_360     = "0to360"
  val LONGITUDE_NEG180    = "-180to180"

  val DEFAULT_OUTPUT_MODE           = OUTPUT_MODE_FLAT
  val DEFAULT_MESSAGES_PER_PARTITION = 20
  val DEFAULT_LONGITUDE_RANGE       = LONGITUDE_0_360
  val DEFAULT_USE_INDEX             = true

  /**
   * Parse options from the Spark DataSource V2 options map.
   * The "paths" key is populated automatically by Spark from .load() arguments.
   */
  def parse(options: CaseInsensitiveStringMap): GribOptions = {
    val paths = Option(options.get("paths"))
      .map(p => p.stripPrefix("[").stripSuffix("]").split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).toSeq)
      .orElse(Option(options.get("path")).map(p => Seq(p)))
      .getOrElse(Seq.empty)

    val variables = Option(options.get("variables"))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet)
      .getOrElse(Set.empty)

    val levelType = Option(options.get("levelType")).map(_.trim).filter(_.nonEmpty)

    val outputMode = Option(options.get("outputMode"))
      .map(_.trim.toLowerCase)
      .getOrElse(DEFAULT_OUTPUT_MODE)
    require(
      outputMode == OUTPUT_MODE_FLAT || outputMode == OUTPUT_MODE_GRIDDED,
      s"Invalid outputMode '$outputMode'. Must be '$OUTPUT_MODE_FLAT' or '$OUTPUT_MODE_GRIDDED'."
    )

    val messagesPerPartition = Option(options.get("messagesPerPartition"))
      .map { s =>
        val n = s.trim.toInt
        require(n > 0, s"messagesPerPartition must be > 0, got $n")
        n
      }
      .getOrElse(DEFAULT_MESSAGES_PER_PARTITION)

    val longitudeRange = Option(options.get("longitudeRange"))
      .map(_.trim)
      .getOrElse(DEFAULT_LONGITUDE_RANGE)
    require(
      longitudeRange == LONGITUDE_0_360 || longitudeRange == LONGITUDE_NEG180,
      s"Invalid longitudeRange '$longitudeRange'. Must be '$LONGITUDE_0_360' or '$LONGITUDE_NEG180'."
    )

    val useIndex = Option(options.get("useIndex"))
      .map(_.trim.toBoolean)
      .getOrElse(DEFAULT_USE_INDEX)

    GribOptions(
      paths = paths,
      variables = variables,
      levelType = levelType,
      outputMode = outputMode,
      messagesPerPartition = messagesPerPartition,
      longitudeRange = longitudeRange,
      useIndex = useIndex
    )
  }
}
