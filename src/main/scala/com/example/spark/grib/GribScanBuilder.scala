package com.example.spark.grib

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
 * Builds a GribScan, optionally accepting pushed-down filters on
 * variable_short_name, level_type, level_value, reference_time, forecast_time.
 */
class GribScanBuilder(
    options: GribOptions,
    schema: StructType
) extends ScanBuilder with SupportsPushDownFilters {

  private var acceptedFilters: Array[Filter] = Array.empty
  private var postScanFilters: Array[Filter] = Array.empty

  /**
   * Determine which filters can be pushed down to the GRIB reader.
   *
   * A filter may only be pushed if the reader is guaranteed to fully evaluate
   * it — otherwise Spark would drop it from post-scan evaluation and the
   * predicate would be silently ignored. [[GribFilters.isSupported]] is the
   * single source of truth for that set, kept in lock-step with
   * [[GribFilters.passes]] (the reader's exact evaluation).
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, remaining) = filters.partition(GribFilters.isSupported)
    acceptedFilters = pushed
    postScanFilters = remaining
    // Return the filters that Spark must still apply (post-scan)
    remaining
  }

  override def pushedFilters(): Array[Filter] = acceptedFilters

  override def build(): Scan = {
    new GribScan(options, schema, acceptedFilters)
  }
}
