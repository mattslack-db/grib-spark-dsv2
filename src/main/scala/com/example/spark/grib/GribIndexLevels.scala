package com.example.spark.grib

/** A level as the decoder reports it: `level_type` and `level_value`. */
case class IndexLevel(levelType: String, value: Double)

/**
 * Maps the level text of a wgrib2 `.idx` line (e.g. "500 mb") to the
 * `level_type`/`level_value` netCDF-Java decodes for that GRIB2 message
 * ("Isobaric surface", 50000.0 Pa), so `.idx` entries can be pruned on level
 * filters before decoding.
 *
 * For a layer ("0.33-1 sigma layer") that is its first bound, which is what
 * the decoder puts in `level_value` (the second goes in `level_value2`).
 * Unrecognised text returns None, which never prunes. Every mapping is checked
 * against decoded NOAA fixtures in GribIndexLevelsFixtureSpec. GRIB2 only:
 * GRIB1 level names differ.
 */
object GribIndexLevels {

  private val Num = """(-?\d+(?:\.\d+)?(?:e-?\d+)?)"""
  private val Layer = s"""$Num-$Num"""

  private val Isobaric                = s"""$Num mb""".r
  private val HeightAboveGround       = s"""$Num m above ground""".r
  private val HeightLayerAboveGround  = s"""$Layer m above ground""".r
  private val DepthLayerBelowGround   = s"""$Layer m below ground""".r
  private val PressureLayerAboveGround = s"""$Layer mb above ground""".r
  private val AltitudeAboveSeaLevel   = s"""$Num m above mean sea level""".r
  private val Hybrid                  = s"""$Num hybrid level""".r
  private val Sigma                   = s"""$Num sigma level""".r
  private val SigmaLayer              = s"""$Layer sigma layer""".r
  private val PotentialVorticity      = s"""PV=$Num \\(Km\\^2/kg/s\\) surface""".r

  private val PascalsPerMillibar = 100.0

  /** Levels with no number in the `.idx` text; the decoder reports level_value 0 for each. */
  private val Named: Map[String, String] = Map(
    "surface"                                          -> "Ground or water surface",
    "mean sea level"                                   -> "Mean sea level",
    "tropopause"                                       -> "Tropopause",
    "max wind"                                         -> "Maximum wind level",
    "entire atmosphere"                                -> "Entire atmosphere",
    "entire atmosphere (considered as a single layer)" -> "Entire atmosphere layer",
    "planetary boundary layer"                         -> "Planetary Boundary Layer",
    "cloud ceiling"                                    -> "Cloud ceiling",
    "low cloud layer"                                  -> "Low cloud layer",
    "middle cloud layer"                               -> "Middle cloud layer",
    "high cloud layer"                                 -> "High cloud layer",
    "highest tropospheric freezing level"              -> "Highest tropospheric freezing level",
    "0C isotherm"                                      -> "Level of 0 °C isotherm"
  )

  def parse(levelDescription: String): Option[IndexLevel] = levelDescription.trim match {
    case Isobaric(mb)                       => level("Isobaric surface", mb.toDouble * PascalsPerMillibar)
    case HeightAboveGround(m)               => level("Specified height level above ground", m.toDouble)
    case HeightLayerAboveGround(m, _)       => level("Specified height level above ground", m.toDouble)
    case DepthLayerBelowGround(m, _)        => level("Depth below land surface", m.toDouble)
    case PressureLayerAboveGround(mb, _)    =>
      level("Level at specified pressure difference from ground to level", mb.toDouble * PascalsPerMillibar)
    case AltitudeAboveSeaLevel(m)           => level("Specific altitude above mean sea level", m.toDouble)
    case Hybrid(n)                          => level("Hybrid level", n.toDouble)
    case Sigma(s)                           => level("Sigma level", s.toDouble)
    case SigmaLayer(s, _)                   => level("Sigma level", s.toDouble)
    case PotentialVorticity(pv)             => level("Potential vorticity surface", pv.toDouble)
    case text                               => Named.get(text).flatMap(level(_, 0.0))
  }

  private def level(levelType: String, value: Double): Option[IndexLevel] = Some(IndexLevel(levelType, value))
}
