package com.example.spark.grib.unit

import com.example.spark.grib.GribOptions
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.collection.JavaConverters._

class GribOptionsSpec extends AnyFunSuite with Matchers {

  private def makeOptions(pairs: (String, String)*): CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(pairs.toMap.asJava)

  test("defaults should be applied when no options specified") {
    val opts = GribOptions.parse(makeOptions("path" -> "/tmp/test.grib2"))

    opts.outputMode shouldBe GribOptions.OUTPUT_MODE_FLAT
    opts.messagesPerPartition shouldBe None
    opts.maxPartitionBytes shouldBe 128L * 1024 * 1024
    opts.longitudeRange shouldBe GribOptions.LONGITUDE_0_360
    opts.useIndex shouldBe true
    opts.variables shouldBe empty
    opts.levelType shouldBe None
  }

  test("path should be parsed from 'path' option") {
    val opts = GribOptions.parse(makeOptions("path" -> "/data/file.grib2"))
    opts.paths shouldBe Seq("/data/file.grib2")
  }

  test("variables should be parsed as comma-separated set") {
    val opts = GribOptions.parse(makeOptions(
      "path" -> "/tmp/test.grib2",
      "variables" -> "TMP, RH, UGRD"
    ))
    opts.variables shouldBe Set("TMP", "RH", "UGRD")
  }

  test("empty variables string should result in empty set") {
    val opts = GribOptions.parse(makeOptions(
      "path" -> "/tmp/test.grib2",
      "variables" -> ""
    ))
    opts.variables shouldBe empty
  }

  test("levelType should be parsed") {
    val opts = GribOptions.parse(makeOptions(
      "path" -> "/tmp/test.grib2",
      "levelType" -> "isobaricInhPa"
    ))
    opts.levelType shouldBe Some("isobaricInhPa")
  }

  test("outputMode 'gridded' should be accepted") {
    val opts = GribOptions.parse(makeOptions(
      "path" -> "/tmp/test.grib2",
      "outputMode" -> "gridded"
    ))
    opts.outputMode shouldBe GribOptions.OUTPUT_MODE_GRIDDED
  }

  test("outputMode should be case-insensitive") {
    val opts = GribOptions.parse(makeOptions(
      "path" -> "/tmp/test.grib2",
      "outputMode" -> "FLAT"
    ))
    opts.outputMode shouldBe GribOptions.OUTPUT_MODE_FLAT
  }

  test("invalid outputMode should throw") {
    an[IllegalArgumentException] should be thrownBy {
      GribOptions.parse(makeOptions(
        "path" -> "/tmp/test.grib2",
        "outputMode" -> "invalid"
      ))
    }
  }

  test("messagesPerPartition should be parsed") {
    val opts = GribOptions.parse(makeOptions(
      "path" -> "/tmp/test.grib2",
      "messagesPerPartition" -> "50"
    ))
    opts.messagesPerPartition shouldBe Some(50)
  }

  test("messagesPerPartition <= 0 should throw") {
    an[IllegalArgumentException] should be thrownBy {
      GribOptions.parse(makeOptions(
        "path" -> "/tmp/test.grib2",
        "messagesPerPartition" -> "0"
      ))
    }
  }

  test("longitudeRange '-180to180' should be accepted") {
    val opts = GribOptions.parse(makeOptions(
      "path" -> "/tmp/test.grib2",
      "longitudeRange" -> "-180to180"
    ))
    opts.longitudeRange shouldBe GribOptions.LONGITUDE_NEG180
  }

  test("invalid longitudeRange should throw") {
    an[IllegalArgumentException] should be thrownBy {
      GribOptions.parse(makeOptions(
        "path" -> "/tmp/test.grib2",
        "longitudeRange" -> "invalid"
      ))
    }
  }

  test("useIndex false should be parsed") {
    val opts = GribOptions.parse(makeOptions(
      "path" -> "/tmp/test.grib2",
      "useIndex" -> "false"
    ))
    opts.useIndex shouldBe false
  }

  test("options should be case-insensitive") {
    val opts = GribOptions.parse(makeOptions(
      "PATH" -> "/tmp/test.grib2",
      "VARIABLES" -> "TMP",
      "OUTPUTMODE" -> "flat"
    ))
    opts.paths shouldBe Seq("/tmp/test.grib2")
    opts.variables shouldBe Set("TMP")
  }

  test("maxPartitionBytes should be parsed, and must be positive") {
    GribOptions.parse(makeOptions("path" -> "/tmp/t.grib2", "maxPartitionBytes" -> "1048576"))
      .maxPartitionBytes shouldBe 1048576L
    an[IllegalArgumentException] should be thrownBy
      GribOptions.parse(makeOptions("path" -> "/tmp/t.grib2", "maxPartitionBytes" -> "0"))
  }

  test("maxPartitionBytes accepts Spark-style size strings") {
    def parsed(v: String) = GribOptions.parse(makeOptions("path" -> "/tmp/t.grib2", "maxPartitionBytes" -> v)).maxPartitionBytes
    parsed("64m") shouldBe 64L * 1024 * 1024
    parsed("128MB") shouldBe 128L * 1024 * 1024
    parsed("1g") shouldBe 1024L * 1024 * 1024
    parsed("4096") shouldBe 4096L
  }

  test("an unparseable maxPartitionBytes gives a clear error") {
    val e = the[IllegalArgumentException] thrownBy
      GribOptions.parse(makeOptions("path" -> "/tmp/t.grib2", "maxPartitionBytes" -> "lots"))
    e.getMessage should include("maxPartitionBytes")
    e.getMessage should include("64m")
  }
}

