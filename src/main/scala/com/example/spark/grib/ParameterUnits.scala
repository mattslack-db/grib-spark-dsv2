package com.example.spark.grib

/**
 * Corrections to parameter units from netCDF-Java 5.6's bundled tables.
 *
 * NCEP's local aerosol parameters (discipline 0, category 13, numbers 192-195:
 * PMTC, PMTF, LPMTF, LIPMF) are in µg m-3 per NCEP's table 4.2-0-13
 * (https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_table4-2-0-13.shtml),
 * but netCDF-Java's copy lists them as "g m-3": the micro sign was lost when the
 * table was imported. A correction only applies while the decoded units are
 * still the known-bad value, so a fixed netCDF-Java table takes over unchanged.
 */
object ParameterUnits {

  private val NcepCentre = 7
  private val NcepMissingMicro: Set[(Int, Int, Int)] = Set((0, 13, 192), (0, 13, 193), (0, 13, 194), (0, 13, 195))

  // "g m-3" or "g.m-3" (netCDF-Java uses either separator), alone or inside "log10 (...)".
  private val GramsPerCubicMetre = """(^|\()g([ .]m-3)""".r
  private val Micro = "µ"

  def resolve(centre: Int, discipline: Int, category: Int, number: Int, units: String): String =
    if (centre == NcepCentre && NcepMissingMicro((discipline, category, number)))
      GramsPerCubicMetre.replaceFirstIn(units, s"$$1${Micro}g$$2")
    else units
}
