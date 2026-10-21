package com.example.spark.grib.unit

import com.example.spark.grib.GribPartitionReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GribPartitionReaderPathSpec extends AnyFunSuite with Matchers {

  test("plain absolute paths are read directly from the local filesystem") {
    GribPartitionReader.localPathOf("/Volumes/main/grib/raw/gfs.f000") shouldBe
      Some("/Volumes/main/grib/raw/gfs.f000")
  }

  test("file: URIs are converted to local paths") {
    GribPartitionReader.localPathOf("file:/tmp/gfs.grib2") shouldBe Some("/tmp/gfs.grib2")
    GribPartitionReader.localPathOf("file:///tmp/gfs.grib2") shouldBe Some("/tmp/gfs.grib2")
  }

  test("local paths containing spaces are supported") {
    GribPartitionReader.localPathOf("/Volumes/main/grib/my files/gfs.f000") shouldBe
      Some("/Volumes/main/grib/my files/gfs.f000")
  }

  test("dbfs: paths (as returned when listing UC volumes on Databricks) are treated as remote") {
    GribPartitionReader.localPathOf("dbfs:/Volumes/main/grib/raw/gfs.f000") shouldBe None
  }

  test("any other filesystem scheme is treated as remote") {
    Seq(
      "s3a://bucket/key",
      "s3://bucket/key",
      "gs://bucket/key",
      "hdfs://nn/key",
      "abfss://c@acct.dfs.core.windows.net/key",
      "wasbs://c@acct.blob.core.windows.net/key"
    ).foreach { p =>
      GribPartitionReader.localPathOf(p) shouldBe None
    }
  }
}
