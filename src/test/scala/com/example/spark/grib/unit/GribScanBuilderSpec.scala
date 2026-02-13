package com.example.spark.grib.unit

import com.example.spark.grib.{GribOptions, GribScanBuilder, GribSchemas}
import org.apache.spark.sql.sources._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GribScanBuilderSpec extends AnyFunSuite with Matchers {

  private def builder(): GribScanBuilder = {
    val opts = GribOptions(
      paths = Seq("/tmp/test.grib2"),
      variables = Set.empty,
      levelType = None,
      outputMode = "flat",
      messagesPerPartition = 20,
      longitudeRange = "0to360",
      useIndex = true
    )
    new GribScanBuilder(opts, GribSchemas.flatSchema)
  }

  test("EqualTo on variable_short_name should be pushed") {
    val b = builder()
    val filters = Array[Filter](EqualTo("variable_short_name", "TMP"))
    val remaining = b.pushFilters(filters)

    remaining shouldBe empty
    b.pushedFilters() should contain(EqualTo("variable_short_name", "TMP"))
  }

  test("In on variable_short_name should be pushed") {
    val b = builder()
    val filters = Array[Filter](In("variable_short_name", Array("TMP", "RH")))
    val remaining = b.pushFilters(filters)

    remaining shouldBe empty
    b.pushedFilters() should have length 1
  }

  test("EqualTo on level_type should be pushed") {
    val b = builder()
    val filters = Array[Filter](EqualTo("level_type", "isobaricInhPa"))
    val remaining = b.pushFilters(filters)

    remaining shouldBe empty
    b.pushedFilters() should have length 1
  }

  test("GreaterThan on level_value should be pushed") {
    val b = builder()
    val filters = Array[Filter](GreaterThan("level_value", 500.0))
    val remaining = b.pushFilters(filters)

    remaining shouldBe empty
    b.pushedFilters() should have length 1
  }

  test("EqualTo on latitude should NOT be pushed") {
    val b = builder()
    val filters = Array[Filter](EqualTo("latitude", 40.0))
    val remaining = b.pushFilters(filters)

    remaining should have length 1
    b.pushedFilters() shouldBe empty
  }

  test("EqualTo on longitude should NOT be pushed") {
    val b = builder()
    val filters = Array[Filter](EqualTo("longitude", -105.0))
    val remaining = b.pushFilters(filters)

    remaining should have length 1
    b.pushedFilters() shouldBe empty
  }

  test("EqualTo on value should NOT be pushed") {
    val b = builder()
    val filters = Array[Filter](GreaterThan("value", 300.0))
    val remaining = b.pushFilters(filters)

    remaining should have length 1
    b.pushedFilters() shouldBe empty
  }

  test("mixed pushable and non-pushable filters should be correctly partitioned") {
    val b = builder()
    val filters = Array[Filter](
      EqualTo("variable_short_name", "TMP"),
      GreaterThan("level_value", 500.0),
      EqualTo("latitude", 40.0),
      LessThan("value", 300.0)
    )
    val remaining = b.pushFilters(filters)

    remaining should have length 2
    remaining.map(_.toString) should contain("EqualTo(latitude,40.0)")
    b.pushedFilters() should have length 2
  }

  test("And of pushable filters should be pushed") {
    val b = builder()
    val filters = Array[Filter](
      And(EqualTo("variable_short_name", "TMP"), EqualTo("level_type", "surface"))
    )
    val remaining = b.pushFilters(filters)

    remaining shouldBe empty
    b.pushedFilters() should have length 1
  }

  test("And with non-pushable child should NOT be pushed") {
    val b = builder()
    val filters = Array[Filter](
      And(EqualTo("variable_short_name", "TMP"), EqualTo("latitude", 40.0))
    )
    val remaining = b.pushFilters(filters)

    remaining should have length 1
    b.pushedFilters() shouldBe empty
  }

  test("IsNotNull on pushable column should be pushed") {
    val b = builder()
    val filters = Array[Filter](IsNotNull("variable_short_name"))
    val remaining = b.pushFilters(filters)

    remaining shouldBe empty
    b.pushedFilters() should have length 1
  }

  test("filters on reference_time and forecast_time should be pushed") {
    val b = builder()
    val filters = Array[Filter](
      GreaterThan("reference_time", java.sql.Timestamp.valueOf("2024-01-01 00:00:00")),
      LessThan("forecast_time", java.sql.Timestamp.valueOf("2024-01-02 00:00:00"))
    )
    val remaining = b.pushFilters(filters)

    remaining shouldBe empty
    b.pushedFilters() should have length 2
  }

  test("filter on discipline should be pushed") {
    val b = builder()
    val filters = Array[Filter](EqualTo("discipline", 0))
    val remaining = b.pushFilters(filters)

    remaining shouldBe empty
    b.pushedFilters() should have length 1
  }
}
