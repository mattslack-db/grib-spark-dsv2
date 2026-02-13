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
   * Pushable filters: variable_short_name, level_type, level_value,
   * reference_time, forecast_time.
   * Non-pushable: latitude, longitude, value, message_index, file_path, etc.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, remaining) = filters.partition(canPush)
    acceptedFilters = pushed
    postScanFilters = remaining
    // Return the filters that Spark must still apply (post-scan)
    remaining
  }

  override def pushedFilters(): Array[Filter] = acceptedFilters

  override def build(): Scan = {
    new GribScan(options, schema, acceptedFilters)
  }

  /**
   * Check whether a filter can be pushed down to the data source.
   */
  private def canPush(filter: Filter): Boolean = filter match {
    case EqualTo(attr, _)            => isPushableColumn(attr)
    case EqualNullSafe(attr, _)      => isPushableColumn(attr)
    case In(attr, _)                 => isPushableColumn(attr)
    case GreaterThan(attr, _)        => isPushableColumn(attr)
    case GreaterThanOrEqual(attr, _) => isPushableColumn(attr)
    case LessThan(attr, _)           => isPushableColumn(attr)
    case LessThanOrEqual(attr, _)    => isPushableColumn(attr)
    case IsNotNull(attr)             => isPushableColumn(attr)
    case IsNull(attr)                => isPushableColumn(attr)
    case And(left, right)            => canPush(left) && canPush(right)
    case Or(left, right)             => canPush(left) && canPush(right)
    case Not(child)                  => canPush(child)
    case _                           => false
  }

  private val pushableColumns = Set(
    "variable_short_name",
    "level_type",
    "level_value",
    "reference_time",
    "forecast_time",
    "discipline"
  )

  private def isPushableColumn(attr: String): Boolean =
    pushableColumns.contains(attr.toLowerCase)
}
