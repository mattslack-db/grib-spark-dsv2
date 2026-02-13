package com.example.spark.grib.integration

import com.example.spark.grib.SparkTestBase
import com.example.spark.grib.tags.SlowTest
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for filter push-down behavior.
 * Verifies that pushed filters reduce the amount of data read.
 */
class FilterPushDownSpec extends SparkTestBase with Matchers {

  private lazy val gribFile: Option[String] = {
    try {
      Some(testResource("gfs_sample.grib2"))
    } catch {
      case _: Exception => None
    }
  }

  test("filtering by variable_short_name reduces row count", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val dfAll = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)

    val dfFiltered = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)
      .where("variable_short_name = 'TMP'")

    val allCount = dfAll.count()
    val filteredCount = dfFiltered.count()

    filteredCount should be > 0L
    filteredCount should be < allCount
  }

  test("filtering by level_type reduces row count", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val dfAll = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)

    val dfFiltered = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)
      .where("level_type = 'surface'")

    dfFiltered.count() should be < dfAll.count()
  }

  test("IN filter on variable_short_name works correctly", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)
      .where("variable_short_name IN ('TMP', 'RH')")

    val variables = df.select("variable_short_name").distinct().collect().map(_.getString(0)).toSet
    variables.foreach { v =>
      Set("TMP", "RH") should contain(v)
    }
  }

  test("combined variable and level filters", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val dfVar = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)
      .where("variable_short_name = 'TMP'")

    val dfBoth = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)
      .where("variable_short_name = 'TMP' AND level_value > 500")

    dfBoth.count() should be <= dfVar.count()
  }
}
