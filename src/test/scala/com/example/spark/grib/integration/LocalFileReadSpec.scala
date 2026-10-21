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

  test("index-based reads number messages by their position in the file", SlowTest) {
    assume(gribFile.isDefined && idxFile.isDefined, "Test fixtures not found. Run download_test_fixtures.sh")

    def messageKeys(useIndex: Boolean) = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("useIndex", useIndex.toString)
      .option("messagesPerPartition", "7")
      .option("variables", "TMP,RH")
      .load(gribFile.get)
      .select("message_index", "variable_short_name", "level_type", "level_value")
      .collect()
      .map(r => (r.getInt(0), r.getString(1), r.getString(2), r.getDouble(3)))
      .sortBy(_._1)
      .toSeq

    val wholeFile = messageKeys(useIndex = false)
    val indexed = messageKeys(useIndex = true)

    // Spans several partitions, so a per-partition counter would repeat indices.
    indexed.size should be > 7
    indexed.map(_._1).distinct.size shouldBe indexed.size
    indexed shouldBe wholeFile
  }

  test("directory and glob loads read NOAA-named files and skip sidecars", SlowTest) {
    assume(gribFile.isDefined && idxFile.isDefined, "Test fixtures not found. Run download_test_fixtures.sh")

    val dir = java.nio.file.Files.createTempDirectory("grib_dir_")
    val noaaName = "gfs.t00z.pgrb2.1p00.f000" // dots, but no GRIB extension
    java.nio.file.Files.copy(java.nio.file.Paths.get(gribFile.get), dir.resolve(noaaName))
    java.nio.file.Files.copy(java.nio.file.Paths.get(idxFile.get), dir.resolve(s"$noaaName.idx"))
    java.nio.file.Files.write(dir.resolve(s"$noaaName.md5"), "not grib".getBytes)
    java.nio.file.Files.write(dir.resolve("_SUCCESS"), Array.emptyByteArray)

    def tmpRows(path: String) = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("variables", "TMP")
      .load(path)
      .select("file_path", "message_index")
      .collect()

    val expected = tmpRows(gribFile.get).length
    expected should be > 0
    for (path <- Seq(dir.toString, s"$dir/*")) {
      val rows = tmpRows(path)
      rows.length shouldBe expected
      rows.map(_.getString(0)).distinct.map(_.split('/').last) shouldBe Array(noaaName)
    }
  }

  test("byte-sized index partitions decode exactly the messages of a whole-file read", SlowTest) {
    assume(gribFile.isDefined && idxFile.isDefined, "Test fixtures not found. Run download_test_fixtures.sh")

    def read(useIndex: Boolean) = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("useIndex", useIndex.toString)
      .option("maxPartitionBytes", (4L * 1024 * 1024).toString) // ~40 MB file -> ~10 partitions
      .load(gribFile.get)

    val indexed = read(useIndex = true)
    indexed.rdd.getNumPartitions should be > 5

    def keys(df: org.apache.spark.sql.DataFrame) =
      df.select("message_index", "variable_short_name", "level_type", "level_value").collect().map(_.toString).sorted.toSeq
    keys(indexed) shouldBe keys(read(useIndex = false))
  }
}

