package com.example.spark.grib.integration

import com.example.spark.grib.SparkTestBase
import com.example.spark.grib.tags.SlowTest
import org.scalatest.matchers.should.Matchers

/**
 * Integration test: end-to-end read of a local GRIB2 file.
 * Requires test fixtures to be downloaded via download_test_fixtures.sh.
 *
 * Tagged as SlowTest because it requires real GRIB files on disk.
 */
class LocalFileReadSpec extends SparkTestBase with Matchers {

  private lazy val gribFile: Option[String] = {
    try {
      Some(testResource("gfs_sample.grib2"))
    } catch {
      case _: Exception => None
    }
  }

  private lazy val idxFile: Option[String] = {
    try {
      Some(testResource("gfs_sample.grib2.idx"))
    } catch {
      case _: Exception => None
    }
  }

  test("read local GRIB2 file with flat schema", SlowTest) {
    assume(gribFile.isDefined, "Test fixture gfs_sample.grib2 not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)

    // Schema should match flat schema
    df.schema.fieldNames should contain allOf(
      "file_path", "variable_short_name", "latitude", "longitude", "value"
    )

    // Should have data
    val count = df.count()
    count should be > 0L

    // Show a sample
    df.select("variable_short_name", "level_type", "level_value", "latitude", "longitude", "value")
      .show(20, truncate = false)
  }

  test("read local GRIB2 file with variable filter", SlowTest) {
    assume(gribFile.isDefined, "Test fixture gfs_sample.grib2 not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("useIndex", "false")
      .option("variables", "TMP")
      .load(gribFile.get)

    val variables = df.select("variable_short_name").distinct().collect().map(_.getString(0))
    variables should contain only "TMP"
  }

  test("read local GRIB2 file with gridded output mode", SlowTest) {
    assume(gribFile.isDefined, "Test fixture gfs_sample.grib2 not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("useIndex", "false")
      .option("outputMode", "gridded")
      .load(gribFile.get)

    df.schema.fieldNames should contain allOf("nx", "ny", "values")

    val first = df.first()
    val nx = first.getAs[Int]("nx")
    val ny = first.getAs[Int]("ny")
    val values = first.getAs[Seq[Double]]("values")

    nx should be > 0
    ny should be > 0
    values should have length (nx * ny)
  }

  test("read with index-based partitioning", SlowTest) {
    assume(gribFile.isDefined, "Test fixture gfs_sample.grib2 not found. Run download_test_fixtures.sh")
    assume(idxFile.isDefined, "Test fixture gfs_sample.grib2.idx not found. Run download_test_fixtures.sh")

    // Read without index
    val dfNoIndex = spark.read
      .format("grib")
      .option("useIndex", "false")
      .option("variables", "TMP")
      .load(gribFile.get)
    val countNoIndex = dfNoIndex.count()

    // Read with index
    val dfWithIndex = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("variables", "TMP")
      .load(gribFile.get)
    val countWithIndex = dfWithIndex.count()

    // Both should return the same number of rows for TMP
    countWithIndex shouldBe countNoIndex
  }

  test("SQL push-down filter on variable_short_name", SlowTest) {
    assume(gribFile.isDefined, "Test fixture gfs_sample.grib2 not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("useIndex", "false")
      .load(gribFile.get)

    val filtered = df.where("variable_short_name = 'TMP'")
    val variables = filtered.select("variable_short_name").distinct().collect().map(_.getString(0))
    variables should contain only "TMP"

    // Filtered count should be less than total
    filtered.count() should be < df.count()
  }

  test("longitude normalization -180to180", SlowTest) {
    assume(gribFile.isDefined, "Test fixture gfs_sample.grib2 not found. Run download_test_fixtures.sh")

    val df = spark.read
      .format("grib")
      .option("useIndex", "false")
      .option("longitudeRange", "-180to180")
      .option("variables", "TMP")
      .load(gribFile.get)

    import spark.implicits._
    val minLon = df.select("longitude").as[Double].reduce((a, b) => Math.min(a, b))
    val maxLon = df.select("longitude").as[Double].reduce((a, b) => Math.max(a, b))

    minLon should be >= -180.0
    maxLon should be <= 180.0
  }
}
