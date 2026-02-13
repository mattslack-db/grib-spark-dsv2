package com.example.spark.grib.integration

import com.example.spark.grib.SparkTestBase
import com.example.spark.grib.tags.SlowTest
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for gridded output mode.
 */
class GriddedOutputSpec extends SparkTestBase with Matchers {

  private lazy val gribFile: Option[String] = {
    try {
      Some(testResource("gfs_sample.grib2"))
    } catch {
      case _: Exception => None
    }
  }

  test("gridded mode produces one row per message", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val dfGridded = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("useIndex", "false")
      .load(gribFile.get)

    val dfFlat = spark.read
      .format("grib")
      .option("outputMode", "flat")
      .option("useIndex", "false")
      .load(gribFile.get)

    // Gridded should have far fewer rows than flat
    val griddedCount = dfGridded.count()
    val flatCount = dfFlat.count()

    griddedCount should be > 0L
    griddedCount should be < flatCount
  }

  test("gridded schema has expected columns", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("useIndex", "false")
      .load(gribFile.get)

    df.schema.fieldNames should contain allOf("nx", "ny", "values", "lat_start", "lon_start")
  }

  test("values array length matches nx * ny", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("useIndex", "false")
      .option("variables", "TMP")
      .load(gribFile.get)

    val rows = df.select("nx", "ny", "values").collect()
    rows.length should be > 0

    for (row <- rows) {
      val nx = row.getAs[Int]("nx")
      val ny = row.getAs[Int]("ny")
      val values = row.getAs[Seq[Double]]("values")
      values.length shouldBe (nx * ny)
    }
  }

  test("gridded mode with variable filter", SlowTest) {
    assume(gribFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("useIndex", "false")
      .option("variables", "TMP")
      .load(gribFile.get)

    val variables = df.select("variable_short_name").distinct().collect().map(_.getString(0))
    variables should contain only "TMP"
  }
}
