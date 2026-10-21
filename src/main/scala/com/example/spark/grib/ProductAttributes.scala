package com.example.spark.grib

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import ucar.nc2.grib.grib2.Grib2Pds
import ucar.nc2.grib.grib2.table.Grib2Tables

/**
 * Template-specific identity of a GRIB2 message, beyond variable/level/time.
 * Each field is None when the message's product template doesn't carry it; a
 * template can carry several (e.g. 4.11 is an ensemble member's accumulation,
 * so it has both `ensemble` and `statistic`).
 *
 * Supporting another netCDF-Java PDS interface means adding a field here, its
 * schema and row conversion, and a line in [[ProductAttributes.fromGrib2]].
 */
case class ProductAttributes(
    aerosol: Option[AerosolAttributes] = None,
    ensemble: Option[EnsembleAttributes] = None,
    ensembleDerived: Option[EnsembleDerivedAttributes] = None,
    probability: Option[ProbabilityAttributes] = None,
    percentile: Option[Int] = None,
    statistic: Option[StatisticAttributes] = None
)

/**
 * Aerosol identity from templates 4.46/4.48. Sizes and wavelengths are in
 * metres, with GRIB2's own interval encoding (code table 4.91): the interval
 * type says how the first and second limits combine (e.g. 2 = from the first,
 * up to but excluding the second; 11 = equal to the first). A limit the
 * interval type doesn't use is None.
 */
case class AerosolAttributes(
    aerosolType: Int,
    typeName: Option[String],
    sizeIntervalType: Option[Int],
    sizeFirstM: Option[Double],
    sizeSecondM: Option[Double],
    wavelengthIntervalType: Option[Int],
    wavelengthFirstM: Option[Double],
    wavelengthSecondM: Option[Double]
)

/** One ensemble member (templates 4.1, 4.11). Perturbation type is code table 4.6. */
case class EnsembleAttributes(
    perturbationType: Int,
    perturbationTypeName: Option[String],
    member: Int,
    ensembleSize: Int
)

/** A product derived from all members, e.g. the mean (templates 4.2, 4.12). Type is code table 4.7. */
case class EnsembleDerivedAttributes(
    derivedType: Int,
    typeName: Option[String],
    ensembleSize: Int
)

/**
 * Probability of the parameter being below, above or between limits
 * (templates 4.5, 4.9), in the parameter's units. Type is code table 4.9; a
 * limit the type doesn't use is None.
 */
case class ProbabilityAttributes(
    probabilityType: Int,
    typeName: Option[String],
    lowerLimit: Option[Double],
    upperLimit: Option[Double]
)

/** Statistical processing over a time interval (templates 4.8-4.12). Process is code table 4.10. */
case class StatisticAttributes(
    process: Int,
    processName: Option[String],
    intervalHours: Option[Double]
)

object ProductAttributes {

  val Empty: ProductAttributes = ProductAttributes()

  private val IntervalTypeMissing = 255

  // Code table 4.91: which limits each interval type uses.
  private val UsesFirstLimit = Set(0, 2, 3, 5, 7, 8, 10, 11)
  private val UsesSecondLimit = Set(1, 2, 4, 6, 7, 9, 10)

  // Code table 4.9: which limits each probability type uses.
  private val UsesLowerLimit = Set(0, 2, 3)
  private val UsesUpperLimit = Set(1, 2, 4)

  // ===== Schema =====

  val aerosolSchema: StructType = StructType(Seq(
    StructField("type",                     IntegerType, nullable = false),
    StructField("type_name",                StringType,  nullable = true),
    StructField("size_interval_type",       IntegerType, nullable = true),
    StructField("size_first_m",             DoubleType,  nullable = true),
    StructField("size_second_m",            DoubleType,  nullable = true),
    StructField("wavelength_interval_type", IntegerType, nullable = true),
    StructField("wavelength_first_m",       DoubleType,  nullable = true),
    StructField("wavelength_second_m",      DoubleType,  nullable = true)
  ))

  val ensembleSchema: StructType = StructType(Seq(
    StructField("perturbation_type",      IntegerType, nullable = false),
    StructField("perturbation_type_name", StringType,  nullable = true),
    StructField("member",                 IntegerType, nullable = false),
    StructField("ensemble_size",          IntegerType, nullable = false)
  ))

  val ensembleDerivedSchema: StructType = StructType(Seq(
    StructField("type",          IntegerType, nullable = false),
    StructField("type_name",     StringType,  nullable = true),
    StructField("ensemble_size", IntegerType, nullable = false)
  ))

  val probabilitySchema: StructType = StructType(Seq(
    StructField("type",        IntegerType, nullable = false),
    StructField("type_name",   StringType,  nullable = true),
    StructField("lower_limit", DoubleType,  nullable = true),
    StructField("upper_limit", DoubleType,  nullable = true)
  ))

  val statisticSchema: StructType = StructType(Seq(
    StructField("process",        IntegerType, nullable = false),
    StructField("process_name",   StringType,  nullable = true),
    StructField("interval_hours", DoubleType,  nullable = true)
  ))

  val schema: StructType = StructType(Seq(
    StructField("aerosol",          aerosolSchema,         nullable = true),
    StructField("ensemble",         ensembleSchema,        nullable = true),
    StructField("ensemble_derived", ensembleDerivedSchema, nullable = true),
    StructField("probability",      probabilitySchema,     nullable = true),
    StructField("percentile",       IntegerType,           nullable = true),
    StructField("statistic",        statisticSchema,       nullable = true)
  ))

  // ===== Extraction =====

  def fromGrib2(pds: Grib2Pds, tables: Grib2Tables): ProductAttributes = ProductAttributes(
    aerosol         = as[Grib2Pds.PdsAerosol](pds).map(aerosol(_, tables)),
    ensemble        = as[Grib2Pds.PdsEnsemble](pds).map(ensemble(_, tables)),
    ensembleDerived = as[Grib2Pds.PdsEnsembleDerived](pds).map(ensembleDerived(_, tables)),
    probability     = as[Grib2Pds.PdsProbability](pds).map(probability(_, tables)),
    percentile      = as[Grib2Pds.PdsPercentile](pds).map(_.getPercentileValue),
    statistic       = as[Grib2Pds.PdsInterval](pds).map(_ => statistic(pds, tables))
  )

  private def as[T](pds: Grib2Pds)(implicit tag: scala.reflect.ClassTag[T]): Option[T] =
    tag.unapply(pds)

  private def aerosol(a: Grib2Pds.PdsAerosol, tables: Grib2Tables): AerosolAttributes = {
    val (sizeType, sizeFirst, sizeSecond) =
      interval(a.getAerosolIntervalSizeType, a.getAerosolSize1, a.getAerosolSize2)
    val (wavelengthType, wavelengthFirst, wavelengthSecond) =
      interval(a.getAerosolIntervalWavelengthType, a.getAerosolWavelength1, a.getAerosolWavelength2)
    AerosolAttributes(
      aerosolType = a.getAerosolType,
      typeName = AerosolTypeNames.lookup(a.getAerosolType, tables),
      sizeIntervalType = sizeType,
      sizeFirstM = sizeFirst,
      sizeSecondM = sizeSecond,
      wavelengthIntervalType = wavelengthType,
      wavelengthFirstM = wavelengthFirst,
      wavelengthSecondM = wavelengthSecond
    )
  }

  private def ensemble(e: Grib2Pds.PdsEnsemble, tables: Grib2Tables): EnsembleAttributes = EnsembleAttributes(
    perturbationType = e.getPerturbationType,
    perturbationTypeName = codeTableValue(tables, "4.6", e.getPerturbationType),
    member = e.getPerturbationNumber,
    ensembleSize = e.getNumberEnsembleForecasts
  )

  private def ensembleDerived(d: Grib2Pds.PdsEnsembleDerived, tables: Grib2Tables): EnsembleDerivedAttributes =
    EnsembleDerivedAttributes(
      derivedType = d.getDerivedForecastType,
      typeName = codeTableValue(tables, "4.7", d.getDerivedForecastType),
      ensembleSize = d.getNumberEnsembleForecasts
    )

  private def probability(p: Grib2Pds.PdsProbability, tables: Grib2Tables): ProbabilityAttributes = {
    val probabilityType = p.getProbabilityType
    ProbabilityAttributes(
      probabilityType = probabilityType,
      typeName = codeTableValue(tables, "4.9", probabilityType),
      lowerLimit = Some(p.getProbabilityLowerLimit).filter(v => UsesLowerLimit(probabilityType) && !v.isNaN),
      upperLimit = Some(p.getProbabilityUpperLimit).filter(v => UsesUpperLimit(probabilityType) && !v.isNaN)
    )
  }

  private def statistic(pds: Grib2Pds, tables: Grib2Tables): StatisticAttributes = {
    val process = pds.getStatisticalProcessType
    StatisticAttributes(
      process = process,
      processName = Option(tables.getStatisticName(process)),
      intervalHours = Some(tables.getForecastTimeIntervalSizeInHours(pds)).filter(h => h > 0 && !h.isNaN)
    )
  }

  private def codeTableValue(tables: Grib2Tables, table: String, code: Int): Option[String] =
    Option(tables.getCodeTableValue(table, code))

  /** netCDF-Java returns the interval type as a double; limits it doesn't use are dropped. */
  private def interval(rawType: Double, first: Double, second: Double): (Option[Int], Option[Double], Option[Double]) = {
    val intervalType = rawType.toInt
    if (intervalType == IntervalTypeMissing) (None, None, None)
    else (
      Some(intervalType),
      Some(first).filter(v => UsesFirstLimit(intervalType) && !v.isNaN),
      Some(second).filter(v => UsesSecondLimit(intervalType) && !v.isNaN)
    )
  }

  // ===== Catalyst rows =====

  /** Catalyst value for the `product` column. */
  def toInternalRow(p: ProductAttributes): InternalRow = InternalRow(
    p.aerosol.map(aerosolRow).orNull,
    p.ensemble.map(ensembleRow).orNull,
    p.ensembleDerived.map(ensembleDerivedRow).orNull,
    p.probability.map(probabilityRow).orNull,
    orNull(p.percentile),
    p.statistic.map(statisticRow).orNull
  )

  private def aerosolRow(a: AerosolAttributes): InternalRow = InternalRow(
    a.aerosolType, utf8(a.typeName),
    orNull(a.sizeIntervalType), orNull(a.sizeFirstM), orNull(a.sizeSecondM),
    orNull(a.wavelengthIntervalType), orNull(a.wavelengthFirstM), orNull(a.wavelengthSecondM)
  )

  private def ensembleRow(e: EnsembleAttributes): InternalRow =
    InternalRow(e.perturbationType, utf8(e.perturbationTypeName), e.member, e.ensembleSize)

  private def ensembleDerivedRow(d: EnsembleDerivedAttributes): InternalRow =
    InternalRow(d.derivedType, utf8(d.typeName), d.ensembleSize)

  private def probabilityRow(p: ProbabilityAttributes): InternalRow =
    InternalRow(p.probabilityType, utf8(p.typeName), orNull(p.lowerLimit), orNull(p.upperLimit))

  private def statisticRow(s: StatisticAttributes): InternalRow =
    InternalRow(s.process, utf8(s.processName), orNull(s.intervalHours))

  private def utf8(o: Option[String]): UTF8String = o.map(UTF8String.fromString).orNull

  private[grib] def orNull(o: Option[Any]): Any = o.orNull
}
