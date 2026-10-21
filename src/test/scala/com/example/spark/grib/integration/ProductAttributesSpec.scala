package com.example.spark.grib.integration

import com.example.spark.grib.SparkTestBase
import com.example.spark.grib.tags.SlowTest
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers

import scala.util.Try

/**
 * Integration tests for product-definition-template columns (pds_template,
 * level_value2, product.*). Requires fixtures from download_test_fixtures.sh.
 */
class ProductAttributesSpec extends SparkTestBase with Matchers {

  // First 15 messages of a GEFS-Aerosols file: one per aerosol species/size bin
  // at hybrid level 1, all product template 4.48.
  private lazy val chemFile: Option[String] = Try(testResource("gefs_chem_sample.grib2")).toOption
  private lazy val gfsFile: Option[String] = Try(testResource("gfs_sample.grib2")).toOption
  // One message per template: 4.1, 4.11, 4.2, 4.12, 4.9, 4.10, 4.8 (see download_test_fixtures.sh).
  private lazy val templatesFile: Option[String] = Try(testResource("templates_sample.grib2")).toOption

  private val SpeciesAndBins = 15
  private val Species = 7 // dust, sea salt, sulphate, 2x organic matter, 2x black carbon
  private val SizeEqualToFirstLimit = 11 // GRIB2 code table 4.91

  private def readChem(useIndex: Boolean) = spark.read
    .format("grib")
    .option("outputMode", "gridded")
    .option("useIndex", useIndex.toString)
    .load(chemFile.get)

  test("aerosol template 4.48 messages expose species and size bins", SlowTest) {
    assume(chemFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val rows = readChem(useIndex = false)
      .select("message_index", "pds_template", "product.aerosol.*")
      .orderBy("message_index")
      .collect()

    rows.length shouldBe SpeciesAndBins
    rows.map(_.getAs[Int]("pds_template")).toSet shouldBe Set(48)
    rows.map(_.getAs[Int]("type")).distinct.length shouldBe Species
    rows.foreach(r => Option(r.getAs[String]("type_name")) should not be empty)

    // Every (species, size bin) pair is distinct, so no two messages collide.
    rows.map(r => (r.getAs[Int]("type"), r.getAs[Double]("size_first_m"))).distinct.length shouldBe SpeciesAndBins

    // Message 0: dust, 0.2–2 µm (`>=2e-07,<2e-06` in the .idx).
    val dust = rows.head
    dust.getAs[String]("type_name").toLowerCase should include("dust")
    dust.getAs[Double]("size_first_m") shouldBe 2e-7 +- 1e-12
    dust.getAs[Double]("size_second_m") shouldBe 2e-6 +- 1e-11

    // Message 10: sulphate, a single size (`=1.39e-07`): no second limit.
    val sulphate = rows(10)
    sulphate.getAs[String]("type_name").toLowerCase should include("sulphate")
    sulphate.getAs[Int]("size_interval_type") shouldBe SizeEqualToFirstLimit
    sulphate.getAs[Double]("size_first_m") shouldBe 1.39e-7 +- 1e-12
    Option(sulphate.getAs[java.lang.Double]("size_second_m")) shouldBe None
  }

  test("product attributes are identical with and without the .idx sidecar", SlowTest) {
    assume(chemFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    def keys(useIndex: Boolean) = readChem(useIndex)
      .select("message_index", "product")
      .collect()
      .map(r => (r.getInt(0), r.get(1)))
      .sortBy(_._1)
      .toSeq

    keys(useIndex = true) shouldBe keys(useIndex = false)
  }

  test("filters on nested product fields are applied, not silently dropped", SlowTest) {
    assume(chemFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val df = spark.read.format("grib").option("outputMode", "gridded").load(chemFile.get)
    val dustName = df.where(col("message_index") === 0).select("product.aerosol.type_name").head.getString(0)

    df.where(col("product.aerosol.type_name") === dustName).count() shouldBe 5 // 2 fine + 3 coarse dust bins
  }

  test("non-aerosol GRIB2 messages have a null aerosol struct and template 0", SlowTest) {
    assume(gfsFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    val rows = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("variables", "TMP")
      .load(gfsFile.get)
      .where(col("level_type") === "Specified height level above ground" && col("level_value") === 2.0)
      .select("pds_template", "level_value2", "product.aerosol")
      .collect()

    rows.length shouldBe 1
    rows.head.getAs[Int]("pds_template") shouldBe 0
    Option(rows.head.get(1)) shouldBe None // a single level, not a layer
    Option(rows.head.get(2)) shouldBe None
  }

  test("layer levels expose their second bound in level_value2", SlowTest) {
    assume(gfsFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    // GFS RH has a "0.33-1 sigma layer" message.
    val layer = spark.read
      .format("grib")
      .option("outputMode", "gridded")
      .option("variables", "RH")
      .load(gfsFile.get)
      .where(col("level_type") === "Sigma level" && col("level_value") === 0.33)
      .select("level_value", "level_value2")
      .collect()

    layer.length shouldBe 1
    layer.head.getAs[Double]("level_value2") shouldBe 1.0 +- 1e-9
  }

  test("aerosol particulate matter is reported in NCEP's documented units", SlowTest) {
    assume(chemFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    readChem(useIndex = false).select("units").distinct().collect().map(_.getString(0)) shouldBe
      Array("\u00b5g.m-3")
  }

  private def templateRows() = spark.read
    .format("grib")
    .option("outputMode", "gridded")
    .option("useIndex", "false")
    .load(templatesFile.get)
    .select("message_index", "pds_template", "product")
    .orderBy("message_index")
    .collect()
    .map(r => (r.getInt(0), r.getInt(1), r.getStruct(2)))

  private val GefsEnsembleSize = 30 // as NCEP encodes it (octet 37), though GEFS also runs a control
  private val PositivelyPerturbed = 3 // code table 4.6
  private val UnweightedMean = 0 // code table 4.7
  private val Accumulation = 1 // code table 4.10

  test("each fixture message has the expected product template", SlowTest) {
    assume(templatesFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")

    templateRows().map(_._2).toSeq shouldBe Seq(1, 11, 2, 12, 9, 10, 8)
  }

  test("ensemble templates expose the member and ensemble size", SlowTest) {
    assume(templatesFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")
    val rows = templateRows()

    for (i <- Seq(0, 1)) { // gep01: "ENS=+1"
      val ens = rows(i)._3.getAs[org.apache.spark.sql.Row]("ensemble")
      ens.getAs[Int]("perturbation_type") shouldBe PositivelyPerturbed
      Option(ens.getAs[String]("perturbation_type_name")) should not be empty
      ens.getAs[Int]("member") shouldBe 1
      ens.getAs[Int]("ensemble_size") shouldBe GefsEnsembleSize
      Option(rows(i)._3.get(rows(i)._3.fieldIndex("ensemble_derived"))) shouldBe None
    }
    for (i <- Seq(2, 3)) { // geavg: "ens mean"
      val derived = rows(i)._3.getAs[org.apache.spark.sql.Row]("ensemble_derived")
      derived.getAs[Int]("type") shouldBe UnweightedMean
      derived.getAs[String]("type_name").toLowerCase should include("mean")
      derived.getAs[Int]("ensemble_size") shouldBe GefsEnsembleSize
      Option(rows(i)._3.get(rows(i)._3.fieldIndex("ensemble"))) shouldBe None
    }
  }

  test("probability and percentile templates expose their threshold", SlowTest) {
    assume(templatesFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")
    val rows = templateRows()

    // NBM "prob >0.254": exceeding 0.254 kg m-2, encoded as either "above upper" or "above lower".
    val prob = rows(4)._3.getAs[org.apache.spark.sql.Row]("probability")
    Option(prob.getAs[String]("type_name")) should not be empty
    val threshold = (prob.getAs[Int]("type"), Option(prob.getAs[java.lang.Double]("lower_limit")),
      Option(prob.getAs[java.lang.Double]("upper_limit")))
    threshold match {
      case (1, None, Some(upper)) => upper.doubleValue shouldBe 0.254 +- 1e-6
      case (3, Some(lower), None) => lower.doubleValue shouldBe 0.254 +- 1e-6
      case other                  => fail(s"Unexpected probability encoding: $other")
    }

    rows(5)._3.getAs[Int]("percentile") shouldBe 1 // NBM "1% level"
    Seq(0, 1, 2, 3, 4, 6).foreach(i => Option(rows(i)._3.get(rows(i)._3.fieldIndex("percentile"))) shouldBe None)
  }

  test("statistically processed templates expose the process and interval", SlowTest) {
    assume(templatesFile.isDefined, "Test fixture not found. Run download_test_fixtures.sh")
    val rows = templateRows()

    for (i <- Seq(1, 3, 4, 5, 6)) { // all 6-hour APCP accumulations
      val stat = rows(i)._3.getAs[org.apache.spark.sql.Row]("statistic")
      stat.getAs[Int]("process") shouldBe Accumulation
      stat.getAs[String]("process_name").toLowerCase should include("accumulation")
      stat.getAs[Double]("interval_hours") shouldBe 6.0
    }
    for (i <- Seq(0, 2)) { // instantaneous TMP
      Option(rows(i)._3.get(rows(i)._3.fieldIndex("statistic"))) shouldBe None
    }
  }
}
