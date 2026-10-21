package com.example.spark.grib.integration

import com.example.spark.grib.SparkTestBase
import com.example.spark.grib.tags.SlowTest
import org.scalatest.matchers.should.Matchers

import scala.util.Try

/**
 * Index-based reads of files whose GRIB messages pack several fields, listed in
 * the .idx as sub-messages (`12.1`, `12.2`) at a shared byte offset.
 */
class IndexSubMessageSpec extends SparkTestBase with Matchers {

  // RAP: 17 GRIB messages, 19 fields; messages 12 and 17 each hold UGRD + VGRD.
  private lazy val rapFile: Option[String] = Try(testResource("rap_submessage_sample.grib2")).toOption

  private val Fields = 19

  private def fieldKeys(useIndex: Boolean, variables: Option[String] = None) = {
    val reader = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("useIndex", useIndex.toString)
      .option("messagesPerPartition", "3")
    variables.fold(reader)(v => reader.option("variables", v))
      .load(rapFile.get)
      .select("message_index", "variable_short_name", "level_value")
      .collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2)))
      .sortBy(_._1)
      .toSeq
  }

  test("an index-based read decodes every field exactly once, numbered as in a whole-file read", SlowTest) {
    assume(rapFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val indexed = fieldKeys(useIndex = true)

    indexed should have length Fields
    indexed.map(_._1) shouldBe (0 until Fields)
    indexed shouldBe fieldKeys(useIndex = false)
  }

  test("variable filters find fields stored as sub-messages", SlowTest) {
    assume(rapFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    for (variable <- Seq("UGRD", "VGRD")) {
      val indexed = fieldKeys(useIndex = true, Some(variable))
      indexed.map(_._2).distinct shouldBe Seq(variable)
      indexed should have length 2 // 100 mb and 125 mb
      indexed shouldBe fieldKeys(useIndex = false, Some(variable))
    }
  }

  test("a malformed .idx is not trusted: the file is read whole instead", SlowTest) {
    assume(rapFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    // Corrupt one line, so its message's bytes would otherwise merge into the previous entry's range.
    val dir = java.nio.file.Files.createTempDirectory("grib_bad_idx_")
    val grib = dir.resolve("rap.grib2")
    java.nio.file.Files.copy(java.nio.file.Paths.get(rapFile.get), grib)
    val lines = scala.io.Source.fromFile(rapFile.get + ".idx").getLines().toVector
    java.nio.file.Files.write(java.nio.file.Paths.get(grib.toString + ".idx"),
      lines.updated(4, "corrupted line").mkString("\n").getBytes)

    val df = spark.read.format("grib").option("outputMode", "gridded").option("variables", "TMP,HGT").load(grib.toString)
    val keys = df.select("message_index", "variable_short_name", "level_value").collect()
      .map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).sortBy(_._1).toSeq

    keys shouldBe fieldKeys(useIndex = false, Some("TMP,HGT"))
    df.rdd.getNumPartitions shouldBe 1 // the whole file, not index-based slices
  }
}

