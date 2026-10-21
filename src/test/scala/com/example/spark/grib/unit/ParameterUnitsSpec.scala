package com.example.spark.grib.unit

import com.example.spark.grib.ParameterUnits
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ParameterUnitsSpec extends AnyFunSuite with Matchers {

  private val Ncep = 7
  private val Ecmwf = 98
  private val Micro = "µ"

  test("restores the micro sign netCDF-Java's NCEP aerosol table drops") {
    ParameterUnits.resolve(Ncep, 0, 13, 192, "g.m-3") shouldBe s"${Micro}g.m-3"
    ParameterUnits.resolve(Ncep, 0, 13, 193, "g m-3") shouldBe s"${Micro}g m-3"
    ParameterUnits.resolve(Ncep, 0, 13, 194, "log10 (g.m-3)") shouldBe s"log10 (${Micro}g.m-3)"
    ParameterUnits.resolve(Ncep, 0, 13, 195, "log10 (g m-3)") shouldBe s"log10 (${Micro}g m-3)"
  }

  test("leaves units alone once netCDF-Java's table is corrected") {
    ParameterUnits.resolve(Ncep, 0, 13, 193, s"${Micro}g.m-3") shouldBe s"${Micro}g.m-3"
    ParameterUnits.resolve(Ncep, 0, 13, 193, "kg.m-3") shouldBe "kg.m-3"
  }

  test("only applies to NCEP's local aerosol parameters") {
    ParameterUnits.resolve(Ecmwf, 0, 13, 193, "g.m-3") shouldBe "g.m-3"
    ParameterUnits.resolve(Ncep, 0, 0, 0, "K") shouldBe "K"
  }
}
