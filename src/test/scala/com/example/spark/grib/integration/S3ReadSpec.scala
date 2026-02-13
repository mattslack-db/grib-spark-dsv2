package com.example.spark.grib.integration

import com.example.spark.grib.SparkTestBase
import com.example.spark.grib.tags.SlowTest
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for reading GRIB files directly from S3.
 * Uses NOAA public buckets with anonymous access.
 *
 * Tests against the GFS 0.25° file (~470 MB, ~400+ messages) to exercise
 * index-based partitioning and push-down predicates at meaningful scale.
 *
 * These tests require network access and are tagged as SlowTest.
 */
class S3ReadSpec extends SparkTestBase with Matchers {

  // GFS 0.25° global analysis — ~470 MB, ~400+ messages, has .idx sidecar
  private val gfs025Path =
    "s3a://noaa-gfs-bdp-pds/gfs.20240101/00/atmos/gfs.t00z.pgrb2.0p25.f000"

  override def beforeAll(): Unit = {
    super.beforeAll()
    configureAnonymousS3()
  }

  // ---------- Basic read tests ----------

  test("read GFS 0.25 from S3 with flat schema", SlowTest) {
    val df = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("variables", "TMP")
      .load(gfs025Path)

    val count = df.count()
    count should be > 0L

    // Should have temperature data
    val variables = df.select("variable_short_name").distinct().collect().map(_.getString(0))
    variables should contain only "TMP"
  }

  test("read GFS 0.25 with gridded output from S3", SlowTest) {
    val df = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("outputMode", "gridded")
      .option("variables", "TMP")
      .load(gfs025Path)

    val count = df.count()
    count should be > 0L

    // Each row should have values array with correct dimensions
    val first = df.first()
    val nx = first.getAs[Int]("nx")
    val ny = first.getAs[Int]("ny")
    nx should be > 0
    ny should be > 0
    // 0.25° global grid: 1440 x 721
    nx shouldBe 1440
    ny shouldBe 721
  }

  // ---------- Index-based partitioning ----------

  test("index-based partitioning reads correct data from S3", SlowTest) {
    val df = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("variables", "TMP")
      .option("messagesPerPartition", "5")
      .load(gfs025Path)

    val count = df.count()
    count should be > 0L

    // All rows should be TMP
    val variables = df.select("variable_short_name").distinct().collect().map(_.getString(0))
    variables should contain only "TMP"
  }

  // ---------- Push-down predicate tests ----------

  test("push-down: variable filter dramatically reduces row count", SlowTest) {
    // Read ALL variables (no filter) in gridded mode to get message count quickly
    val dfAll = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("outputMode", "gridded")
      .load(gfs025Path)
    val allMessageCount = dfAll.count()

    // Read only TMP
    val dfTmp = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("outputMode", "gridded")
      .option("variables", "TMP")
      .load(gfs025Path)
    val tmpMessageCount = dfTmp.count()

    // GFS has ~400 messages total, TMP should be ~30-40 (one per pressure level + surface)
    allMessageCount should be > 100L
    tmpMessageCount should be > 0L
    tmpMessageCount should be < (allMessageCount / 5) // TMP is a small fraction of all variables

    println(s"Total messages: $allMessageCount, TMP messages: $tmpMessageCount " +
      s"(${(tmpMessageCount * 100.0 / allMessageCount).toInt}% of total)")
  }

  test("push-down: SQL WHERE on variable_short_name filters correctly", SlowTest) {
    val df = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("outputMode", "gridded")
      .load(gfs025Path)
      .where("variable_short_name = 'HGT'")

    val count = df.count()
    count should be > 0L

    val variables = df.select("variable_short_name").distinct().collect().map(_.getString(0))
    variables should contain only "HGT"
  }

  test("push-down: IN filter on multiple variables", SlowTest) {
    val df = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("outputMode", "gridded")
      .load(gfs025Path)
      .where("variable_short_name IN ('TMP', 'RH', 'UGRD')")

    val count = df.count()
    count should be > 0L

    val variables = df.select("variable_short_name").distinct().collect().map(_.getString(0)).toSet
    variables.foreach { v =>
      Set("TMP", "RH", "UGRD") should contain(v)
    }
    // Should have found at least 2 of the 3
    variables.size should be >= 2
  }

  test("push-down: level_value filter narrows results", SlowTest) {
    // All TMP messages
    val dfAllTmp = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("outputMode", "gridded")
      .option("variables", "TMP")
      .load(gfs025Path)
    val allTmpCount = dfAllTmp.count()

    // Only TMP at 500 hPa
    val df500 = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("outputMode", "gridded")
      .option("variables", "TMP")
      .load(gfs025Path)
      .where("level_value = 500.0")
    val count500 = df500.count()

    allTmpCount should be > 1L
    count500 shouldBe 1L // Exactly one TMP message at 500 hPa

    // Verify the level value
    val row = df500.first()
    row.getAs[Double]("level_value") shouldBe 500.0
  }

  test("push-down: combined variable + level range filter", SlowTest) {
    val df = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("outputMode", "gridded")
      .option("variables", "TMP")
      .load(gfs025Path)
      .where("level_value >= 200 AND level_value <= 700")

    val count = df.count()
    count should be > 0L

    // All results should be TMP with level between 200-700
    df.collect().foreach { row =>
      row.getAs[String]("variable_short_name") shouldBe "TMP"
      row.getAs[Double]("level_value") should (be >= 200.0 and be <= 700.0)
    }

    println(s"TMP messages in 200-700 hPa range: $count")
  }

  // ---------- Longitude normalization ----------

  test("longitude normalization -180to180 on 0.25 degree grid", SlowTest) {
    val df = spark.read
      .format("grib")
      .option("useIndex", "true")
      .option("variables", "TMP")
      .option("longitudeRange", "-180to180")
      .load(gfs025Path)
      .where("level_value = 500.0")  // single level to keep it manageable

    import spark.implicits._
    val lons = df.select("longitude").as[Double].collect()
    lons.length should be > 0

    val minLon = lons.min
    val maxLon = lons.max

    minLon should be >= -180.0
    maxLon should be <= 180.0

    println(s"Longitude range: [$minLon, $maxLon]")
  }
}
