package com.example.spark.grib

import org.apache.hadoop.conf.Configuration

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

/**
 * A serializable holder for a Hadoop [[Configuration]], so the driver's
 * configuration — including Spark's `spark.hadoop.*` settings and cloud
 * credentials — can be shipped to executors with the partition reader factory.
 *
 * A bare `new Configuration()` on an executor only loads `core-*.xml` from the
 * classpath and would miss anything configured through the Spark session, which
 * is where S3/ABFS/GCS credentials usually live.
 *
 * Serialization delegates to Hadoop's own `Writable` contract, mirroring
 * Spark's internal (package-private) `SerializableConfiguration`.
 */
class SerializableHadoopConf(@transient var value: Configuration) extends Serializable {

  private def writeObject(out: ObjectOutputStream): Unit =
    try {
      out.defaultWriteObject()
      value.write(out)
    } catch {
      case e: IOException => throw e
    }

  private def readObject(in: ObjectInputStream): Unit =
    try {
      in.defaultReadObject()
      value = new Configuration(false)
      value.readFields(in)
    } catch {
      case e: IOException => throw e
    }
}
