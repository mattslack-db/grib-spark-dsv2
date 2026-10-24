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

  // --- Result-correctness: filtered output must actually obey the predicate.
  // A pushed filter that the reader fails to evaluate would silently return
  // violating rows; these tests catch that, unlike pure count-reduction checks.

  test("every row obeys a pushed variable_short_name filter", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)
      .where("variable_short_name = 'TMP'")

    val offending = df.where("variable_short_name <> 'TMP'").count()
    offending shouldBe 0L
  }

  test("every row obeys a pushed level_value range filter", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)
      .where("level_value > 500")

    // No surviving row may violate the predicate. Nulls are excluded by `>`.
    val offending = df.where("level_value <= 500 OR level_value IS NULL").count()
    offending shouldBe 0L
  }

  test("index-based read returns the same rows as whole-file read", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val dfNoIndex = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)
      .where("variable_short_name = 'TMP'")

    val dfIndex = spark.read
      .format("grib")
      .option("useIndex", "true")
      .load(gribFile.get)
      .where("variable_short_name = 'TMP'")

    // Sub-file index partitioning must not add or drop rows versus a full scan.
    dfIndex.count() shouldBe dfNoIndex.count()
  }

  // GFS cloud mixing ratio: CLMR in NOAA's .idx, CLWMR as netCDF-Java decodes it.
  private val CloudMixingRatioFields = 23L

  private def gridded(useIndex: Boolean) =
    spark.read.format("grib").option("outputMode", "gridded").option("useIndex", useIndex.toString)

  test("the variables option finds a field by its .idx name or its decoded name", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val byIdxName = gridded(useIndex = true).option("variables", "CLMR").load(gribFile.get)
    val byDecodedName = gridded(useIndex = true).option("variables", "CLWMR").load(gribFile.get)

    byIdxName.count() shouldBe CloudMixingRatioFields
    byDecodedName.count() shouldBe CloudMixingRatioFields
    byIdxName.select("variable_short_name").distinct().collect().map(_.getString(0)) shouldBe Array("CLWMR")
  }

  test("a pushed filter on a decoded name whose .idx name differs is not pruned away", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val indexed = gridded(useIndex = true).load(gribFile.get).where("variable_short_name = 'CLWMR'")
    val whole = gridded(useIndex = false).load(gribFile.get).where("variable_short_name = 'CLWMR'")

    whole.count() shouldBe CloudMixingRatioFields
    indexed.count() shouldBe CloudMixingRatioFields
  }
}
