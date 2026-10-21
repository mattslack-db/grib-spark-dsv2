package com.example.spark.grib.unit

import com.example.spark.grib.{GribOptions, GribSchemas}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GribSchemaSpec extends AnyFunSuite with Matchers {

  test("flat schema should have expected columns") {
    val schema = GribSchemas.flatSchema
    schema.fieldNames should contain theSameElementsInOrderAs Seq(
      "file_path", "message_index", "discipline",
      "variable_short_name", "variable_name", "units",
      "level_type", "level_value",
      "reference_time", "forecast_time",
      "latitude", "longitude", "value",
      "pds_template", "level_value2", "product"
    )
  }

  test("flat schema should have correct types") {
    val schema = GribSchemas.flatSchema
    schema("file_path").dataType shouldBe StringType
    schema("message_index").dataType shouldBe IntegerType
    schema("discipline").dataType shouldBe IntegerType
    schema("variable_short_name").dataType shouldBe StringType
    schema("level_value").dataType shouldBe DoubleType
    schema("reference_time").dataType shouldBe TimestampType
    schema("forecast_time").dataType shouldBe TimestampType
    schema("latitude").dataType shouldBe DoubleType
    schema("longitude").dataType shouldBe DoubleType
    schema("value").dataType shouldBe DoubleType
  }

  test("gridded schema should have expected columns") {
    val schema = GribSchemas.griddedSchema
    schema.fieldNames should contain theSameElementsInOrderAs Seq(
      "file_path", "message_index", "discipline",
      "variable_short_name", "variable_name", "units",
      "level_type", "level_value",
      "reference_time", "forecast_time",
      "nx", "ny", "lat_start", "lon_start",
      "lat_increment", "lon_increment", "values",
      "pds_template", "level_value2", "product"
    )
  }

  test("gridded schema values column should be Array[Double]") {
    val schema = GribSchemas.griddedSchema
    schema("values").dataType shouldBe ArrayType(DoubleType, containsNull = true)
    schema("nx").dataType shouldBe IntegerType
    schema("ny").dataType shouldBe IntegerType
  }

  test("forMode should return flat schema for 'flat'") {
    GribSchemas.forMode("flat") shouldBe GribSchemas.flatSchema
  }

  test("forMode should return gridded schema for 'gridded'") {
    GribSchemas.forMode("gridded") shouldBe GribSchemas.griddedSchema
  }

  test("forMode should throw for unknown mode") {
    an[IllegalArgumentException] should be thrownBy {
      GribSchemas.forMode("unknown")
    }
  }

  test("flat schema latitude and longitude should be non-nullable") {
    val schema = GribSchemas.flatSchema
    schema("latitude").nullable shouldBe false
    schema("longitude").nullable shouldBe false
    schema("file_path").nullable shouldBe false
  }

  test("flat schema metadata fields should be nullable") {
    val schema = GribSchemas.flatSchema
    schema("variable_short_name").nullable shouldBe true
    schema("level_type").nullable shouldBe true
    schema("reference_time").nullable shouldBe true
    schema("value").nullable shouldBe true
  }

  test("product-definition columns are nullable and typed, in both modes") {
    for (schema <- Seq(GribSchemas.flatSchema, GribSchemas.griddedSchema)) {
      schema("pds_template").dataType shouldBe IntegerType
      schema("level_value2").dataType shouldBe DoubleType
      Seq("pds_template", "level_value2", "product").foreach(c => schema(c).nullable shouldBe true)

      val product = schema("product").dataType.asInstanceOf[StructType]
      val aerosol = product("aerosol")
      aerosol.nullable shouldBe true
      aerosol.dataType shouldBe StructType(Seq(
        StructField("type", IntegerType, nullable = false),
        StructField("type_name", StringType, nullable = true),
        StructField("size_interval_type", IntegerType, nullable = true),
        StructField("size_first_m", DoubleType, nullable = true),
        StructField("size_second_m", DoubleType, nullable = true),
        StructField("wavelength_interval_type", IntegerType, nullable = true),
        StructField("wavelength_first_m", DoubleType, nullable = true),
        StructField("wavelength_second_m", DoubleType, nullable = true)
      ))

      product.fieldNames should contain theSameElementsInOrderAs Seq(
        "aerosol", "ensemble", "ensemble_derived", "probability", "percentile", "statistic")
      product.fields.foreach(_.nullable shouldBe true)
      product("percentile").dataType shouldBe IntegerType
      product("ensemble").dataType.asInstanceOf[StructType].fieldNames should contain theSameElementsInOrderAs Seq(
        "perturbation_type", "perturbation_type_name", "member", "ensemble_size")
      product("ensemble_derived").dataType.asInstanceOf[StructType].fieldNames should contain theSameElementsInOrderAs Seq(
        "type", "type_name", "ensemble_size")
      product("probability").dataType.asInstanceOf[StructType].fieldNames should contain theSameElementsInOrderAs Seq(
        "type", "type_name", "lower_limit", "upper_limit")
      product("statistic").dataType.asInstanceOf[StructType].fieldNames should contain theSameElementsInOrderAs Seq(
        "process", "process_name", "interval_hours")
    }
  }
}
