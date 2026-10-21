package com.example.spark.grib

import org.slf4j.LoggerFactory
import ucar.nc2.grib.grib2.table.Grib2Tables

import javax.xml.parsers.DocumentBuilderFactory
import scala.util.{Failure, Success, Try}

/**
 * Names for GRIB2 code table 4.233 (aerosol type).
 *
 * netCDF-Java 5.6's lookup returns nothing for this table: its WMO copy is
 * empty, and NCEP's local tables don't consult their own 4.233 file. The NCEP
 * file is bundled in the netCDF-Java jar, though, so we read it directly as a
 * fallback.
 */
object AerosolTypeNames {

  private val logger = LoggerFactory.getLogger(getClass)

  private val CodeTable = "4.233"
  private val NcepTableResource = "resources/grib2/ncep/v23.0.0/Table4.233.xml"

  private lazy val ncepNames: Map[Int, String] = loadNcepTable() match {
    case Success(names) => names
    case Failure(e) =>
      logger.warn(s"Could not load aerosol type names from $NcepTableResource; type_name will be null", e)
      Map.empty
  }

  def lookup(code: Int, tables: Grib2Tables): Option[String] =
    Option(tables.getCodeTableValue(CodeTable, code)).orElse(ncepNames.get(code))

  private def loadNcepTable(): Try[Map[Int, String]] = Try {
    val in = Option(getClass.getClassLoader.getResourceAsStream(NcepTableResource))
      .getOrElse(throw new IllegalStateException(s"$NcepTableResource not on the classpath"))
    try {
      val params = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(in)
        .getElementsByTagName("parameter")
      (0 until params.getLength).map { i =>
        val p = params.item(i).asInstanceOf[org.w3c.dom.Element]
        p.getAttribute("code").trim.toInt ->
          p.getElementsByTagName("description").item(0).getTextContent.trim
      }.toMap
    } finally {
      in.close()
    }
  }
}
