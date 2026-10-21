package com.example.spark.grib.unit

import com.example.spark.grib.{GribIndexLevels, IndexLevel}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GribIndexLevelsSpec extends AnyFunSuite with Matchers {

  test("maps single numeric levels to the decoder's GRIB2 level type and value") {
    GribIndexLevels.parse("500 mb") shouldBe Some(IndexLevel("Isobaric surface", 50000.0))
    GribIndexLevels.parse("2 m above ground") shouldBe Some(IndexLevel("Specified height level above ground", 2.0))
    GribIndexLevels.parse("1 hybrid level") shouldBe Some(IndexLevel("Hybrid level", 1.0))
    GribIndexLevels.parse("0.995 sigma level") shouldBe Some(IndexLevel("Sigma level", 0.995))
    GribIndexLevels.parse("1829 m above mean sea level") shouldBe
      Some(IndexLevel("Specific altitude above mean sea level", 1829.0))
    GribIndexLevels.parse("PV=2e-06 (Km^2/kg/s) surface") shouldBe Some(IndexLevel("Potential vorticity surface", 2e-6))
    GribIndexLevels.parse("PV=-2e-06 (Km^2/kg/s) surface") shouldBe Some(IndexLevel("Potential vorticity surface", -2e-6))
  }

  test("maps layers to their first bound, which the decoder reports as level_value") {
    GribIndexLevels.parse("3000-0 m above ground") shouldBe Some(IndexLevel("Specified height level above ground", 3000.0))
    GribIndexLevels.parse("0-0.1 m below ground") shouldBe Some(IndexLevel("Depth below land surface", 0.0))
    GribIndexLevels.parse("30-0 mb above ground") shouldBe
      Some(IndexLevel("Level at specified pressure difference from ground to level", 3000.0))
    GribIndexLevels.parse("0.33-1 sigma layer") shouldBe Some(IndexLevel("Sigma level", 0.33))
  }

  test("maps named levels, whose decoded level_value is 0") {
    Map(
      "surface" -> "Ground or water surface",
      "mean sea level" -> "Mean sea level",
      "tropopause" -> "Tropopause",
      "max wind" -> "Maximum wind level",
      "entire atmosphere" -> "Entire atmosphere",
      "entire atmosphere (considered as a single layer)" -> "Entire atmosphere layer",
      "planetary boundary layer" -> "Planetary Boundary Layer",
      "cloud ceiling" -> "Cloud ceiling",
      "low cloud layer" -> "Low cloud layer",
      "middle cloud layer" -> "Middle cloud layer",
      "high cloud layer" -> "High cloud layer",
      "highest tropospheric freezing level" -> "Highest tropospheric freezing level",
      "0C isotherm" -> "Level of 0 °C isotherm"
    ).foreach { case (text, levelType) =>
      GribIndexLevels.parse(text) shouldBe Some(IndexLevel(levelType, 0.0))
    }
  }

  test("converts fractional millibars to pascals") {
    GribIndexLevels.parse("0.4 mb").map(_.value).get shouldBe 40.0 +- 1e-9
  }

  test("leaves anything it doesn't recognise unknown") {
    Seq(
      "315 K isentropic level",
      "-10C isotherm",
      "1-2 hybrid layer",
      "top of atmosphere",
      "lowest level of the wet bulb zero",
      ""
    ).foreach { text =>
      GribIndexLevels.parse(text) shouldBe None
    }
  }
}
