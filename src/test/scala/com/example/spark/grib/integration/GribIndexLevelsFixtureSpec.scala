package com.example.spark.grib.integration

import com.example.spark.grib.{GribIndexLevels, GribIndexParser, GribMessageDecoder, SparkTestBase}
import com.example.spark.grib.tags.SlowTest
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers

import scala.io.Source
import scala.util.Try

/**
 * Level pruning from `.idx` text is only safe if the mapping in
 * [[GribIndexLevels]] agrees with what the decoder reports. These tests check
 * it against every message of the real NOAA fixtures, and that level-filtered
 * reads give the same rows with and without the index.
 */
class GribIndexLevelsFixtureSpec extends SparkTestBase with Matchers {

  private val Fixtures = Seq("gfs_sample", "gefs_chem_sample", "rap_submessage_sample")
  private def fixture(name: String): Option[(String, String)] =
    Try((testResource(s"$name.grib2"), testResource(s"$name.grib2.idx"))).toOption

  test("every .idx level the parser recognises matches the decoded level", SlowTest) {
    val available = Fixtures.flatMap(fixture)
    assume(available.nonEmpty, "Test fixtures not found. Run download_test_fixtures.sh")

    for ((grib, idx) <- available) {
      val entries = GribIndexParser.parse(Source.fromFile(idx).mkString)
      val decoded = GribMessageDecoder.decodeFile(grib, 0, computeLatLon = false)
      entries.length shouldBe decoded.length

      val recognised = entries.zip(decoded).flatMap { case (e, m) => GribIndexLevels.parse(e.levelDescription).map(_ -> m) }
      recognised should not be empty
      for ((level, m) <- recognised) {
        withClue(s"$grib message ${m.messageIndex}: ") {
          m.levelType shouldBe level.levelType
          m.levelValue shouldBe level.value +- 1e-6 * math.max(1.0, math.abs(level.value))
        }
      }
    }
  }

  test("level-filtered reads return the same rows with and without the index, and skip non-matching levels", SlowTest) {
    val gfs = fixture("gfs_sample")
    assume(gfs.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    def read(useIndex: Boolean) = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("useIndex", useIndex.toString)
      .option("messagesPerPartition", "1") // one partition per message the plan keeps
      .load(gfs.get._1)

    // (level filter, the same filter's variable part alone)
    val filters = Seq(
      (col("variable_short_name") === "TMP" && col("level_value") === 2.0, col("variable_short_name") === "TMP"),
      (col("variable_short_name") === "HGT" && col("level_type") === "Isobaric surface" &&
        col("level_value").isin(50000.0, 85000.0), col("variable_short_name") === "HGT"),
      (col("variable_short_name") === "RH" && col("level_value") > 90000.0, col("variable_short_name") === "RH")
    )
    for ((f, variableOnly) <- filters) {
      def keys(df: org.apache.spark.sql.DataFrame) =
        df.where(f).select("message_index", "variable_short_name", "level_type", "level_value")
          .collect().map(_.toString).sorted.toSeq
      val planned = read(useIndex = true).where(f).rdd.getNumPartitions
      withClue(s"$f: ") {
        keys(read(useIndex = true)) shouldBe keys(read(useIndex = false))
        // Level pruning drops messages variable pruning alone would decode,
        // but keeps every match (and any message whose .idx level is unknown).
        planned should be < read(useIndex = true).where(variableOnly).rdd.getNumPartitions
        planned should be >= read(useIndex = true).where(f).count().toInt
      }
    }
  }
}
