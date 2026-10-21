package com.example.spark.grib

import org.apache.spark.sql.types._

/**
 * Schema definitions for the GRIB data source.
 */
object GribSchemas {

  /**
   * Product-definition columns, appended to both schemas so existing column
   * positions are unchanged. All nullable: GRIB1 messages have none of them,
   * and `product.*` sub-structs are null unless the message's template uses them.
   */
  private lazy val productColumns: Seq[StructField] = Seq(
    StructField("pds_template", IntegerType,               nullable = true),
    StructField("level_value2", DoubleType,                nullable = true),
    StructField("product",      ProductAttributes.schema, nullable = true)
  )

  /**
   * Flat / long-format schema: one row per grid point per message.
   * This is the most flexible format for SQL queries, joins, and aggregations.
   */
  val flatSchema: StructType = StructType(Seq(
    StructField("file_path",           StringType,    nullable = false),
    StructField("message_index",       IntegerType,   nullable = false),
    StructField("discipline",          IntegerType,   nullable = true),
    StructField("variable_short_name", StringType,    nullable = true),
    StructField("variable_name",       StringType,    nullable = true),
    StructField("units",               StringType,    nullable = true),
    StructField("level_type",          StringType,    nullable = true),
    StructField("level_value",         DoubleType,    nullable = true),
    StructField("reference_time",      TimestampType, nullable = true),
    StructField("forecast_time",       TimestampType, nullable = true),
    StructField("latitude",            DoubleType,    nullable = false),
    StructField("longitude",           DoubleType,    nullable = false),
    StructField("value",               DoubleType,    nullable = true)
  ) ++ productColumns)

  /**
   * Gridded / wide-format schema: one row per message with data as an array.
   * Preserves the original grid structure for downstream array processing.
   */
  val griddedSchema: StructType = StructType(Seq(
    StructField("file_path",           StringType,    nullable = false),
    StructField("message_index",       IntegerType,   nullable = false),
    StructField("discipline",          IntegerType,   nullable = true),
    StructField("variable_short_name", StringType,    nullable = true),
    StructField("variable_name",       StringType,    nullable = true),
    StructField("units",               StringType,    nullable = true),
    StructField("level_type",          StringType,    nullable = true),
    StructField("level_value",         DoubleType,    nullable = true),
    StructField("reference_time",      TimestampType, nullable = true),
    StructField("forecast_time",       TimestampType, nullable = true),
    StructField("nx",                  IntegerType,   nullable = false),
    StructField("ny",                  IntegerType,   nullable = false),
    StructField("lat_start",           DoubleType,    nullable = false),
    StructField("lon_start",           DoubleType,    nullable = false),
    StructField("lat_increment",       DoubleType,    nullable = false),
    StructField("lon_increment",       DoubleType,    nullable = false),
    StructField("values",              ArrayType(DoubleType, containsNull = true), nullable = false)
  ) ++ productColumns)

  /** Return the appropriate schema based on output mode. */
  def forMode(outputMode: String): StructType = outputMode match {
    case GribOptions.OUTPUT_MODE_FLAT    => flatSchema
    case GribOptions.OUTPUT_MODE_GRIDDED => griddedSchema
    case other =>
      throw new IllegalArgumentException(s"Unknown output mode: $other")
  }
}
