package com.example.spark.grib.integration

import com.example.spark.grib._
import com.example.spark.grib.tags.SlowTest
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.io.Source
import scala.util.Try

/**
 * An index-based partition of many scattered messages must not hold every
 * span's temp file (and file handle) at once: each is released as soon as its
 * messages have been read.
 */
class PartitionReaderResourcesSpec extends AnyFunSuite with Matchers {

  private def resource(name: String): Option[String] =
    Try(getClass.getClassLoader.getResource(name).getPath).toOption

  private def rangeTempFiles(): Int =
    Option(new File(System.getProperty("java.io.tmpdir")).listFiles())
      .getOrElse(Array.empty)
      .count(_.getName.startsWith("grib_range_"))

  test("a partition of scattered spans holds at most one span's temp file at a time", SlowTest) {
    val (grib, idx) = (resource("gfs_sample.grib2"), resource("gfs_sample.grib2.idx"))
    assume(grib.isDefined && idx.isDefined, "Test fixtures not found. Run download_test_fixtures.sh")

    // Every TMP message, as one partition: TMP messages are scattered through the
    // file, so this is dozens of separate spans.
    val entries = GribIndexParser.parse(Source.fromFile(idx.get).mkString)
    val ranges = GribIndexParser.groupMessages(entries, new File(grib.get).length)
      .filter(_.entries.exists(_.variableName == "TMP"))
      .map(_.range)
    val spans = PartitionPlanning.coalesce(ranges)
    spans.length should be > 20

    val options = GribOptions(
      paths = Seq(grib.get), variables = Set.empty, levelType = None,
      outputMode = GribOptions.OUTPUT_MODE_GRIDDED, messagesPerPartition = None,
      longitudeRange = GribOptions.LONGITUDE_0_360, useIndex = true)
    val reader = new GribPartitionReader(
      GribInputPartition(grib.get, Some(ranges), Array.empty, options.outputMode),
      options,
      new SerializableHadoopConf(new Configuration()))

    val baseline = rangeTempFiles()
    var rows = 0
    var mostTempFiles = 0
    try {
      while (reader.next()) {
        rows += 1
        mostTempFiles = math.max(mostTempFiles, rangeTempFiles() - baseline)
      }
    } finally {
      reader.close()
    }

    rows shouldBe ranges.length
    mostTempFiles should be <= 1
    rangeTempFiles() shouldBe baseline
  }
}
