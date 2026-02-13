package com.example.spark.grib

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util

/**
 * Entry point for the GRIB data source. Registered with Spark via the
 * META-INF/services mechanism under the short name "grib".
 *
 * Usage:
 * {{{
 *   spark.read.format("grib").load("path/to/file.grib2")
 *   spark.read.format("com.example.spark.grib").load("path/to/file.grib2")
 * }}}
 */
class DefaultSource extends TableProvider with DataSourceRegister {

  /** Short name for `.format("grib")` */
  override def shortName(): String = "grib"

  /**
   * Infer the schema by examining options. Since the GRIB schema is fixed
   * (determined solely by the outputMode option), we don't need to actually
   * read a file to infer it.
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val opts = GribOptions.parse(options)
    GribSchemas.forMode(opts.outputMode)
  }

  /**
   * Return a GribTable configured with the user's options.
   */
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val opts = GribOptions.parse(options)
    new GribTable(opts, GribSchemas.forMode(opts.outputMode))
  }
}
