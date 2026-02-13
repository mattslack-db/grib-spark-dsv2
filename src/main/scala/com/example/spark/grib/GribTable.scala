package com.example.spark.grib

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util

/**
 * Represents a GRIB "table" — the collection of GRIB messages across all
 * input files, exposed as a tabular dataset.
 */
class GribTable(
    options: GribOptions,
    tableSchema: StructType
) extends Table with SupportsRead {

  override def name(): String = "grib"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    val caps = new util.HashSet[TableCapability]()
    caps.add(TableCapability.BATCH_READ)
    caps
  }

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder = {
    new GribScanBuilder(options, tableSchema)
  }
}
