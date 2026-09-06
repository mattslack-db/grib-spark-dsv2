package com.example.spark.grib

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

/**
 * Factory that creates GribPartitionReader instances on the executors.
 */
class GribPartitionReaderFactory(
    options: GribOptions,
    serializableConf: SerializableHadoopConf
) extends PartitionReaderFactory with Serializable {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val gribPartition = partition.asInstanceOf[GribInputPartition]
    new GribPartitionReader(gribPartition, options, serializableConf)
  }
}
