package com.example.spark.grib.unit

import com.example.spark.grib.AerosolTypeNames
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import ucar.nc2.grib.grib2.table.Grib2Tables

class AerosolTypeNamesSpec extends AnyFunSuite with Matchers {

  // NCEP (centre 7) tables, as used by GEFS-Aerosols.
  private val ncepTables = Grib2Tables.factory(7, 0, 2, 1, -1)

  test("resolves aerosol types netCDF-Java's own lookup misses") {
    AerosolTypeNames.lookup(62001, ncepTables) shouldBe Some("Dust Dry")
    AerosolTypeNames.lookup(62006, ncepTables) shouldBe Some("Sulphate Dry")
    AerosolTypeNames.lookup(62008, ncepTables) shouldBe Some("Sea Salt Dry")
  }

  test("returns None for codes not in the table") {
    AerosolTypeNames.lookup(65534, ncepTables) shouldBe None
  }
}
