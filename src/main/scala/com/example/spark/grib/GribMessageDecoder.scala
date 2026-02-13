package com.example.spark.grib

import ucar.nc2.grib.grib2.{Grib2RecordScanner, Grib2Record, Grib2Gds}
import ucar.nc2.grib.grib2.Grib2SectionIdentification
import ucar.nc2.grib.grib2.table.Grib2Tables
import ucar.nc2.grib.grib1.{Grib1RecordScanner, Grib1Record, Grib1Gds}
import ucar.nc2.grib.grib1.tables.{Grib1Customizer, Grib1ParamTables}
import ucar.nc2.grib.GdsHorizCoordSys
import ucar.unidata.io.RandomAccessFile
import java.io.IOException
import java.sql.Timestamp
import java.util.{Calendar, GregorianCalendar, TimeZone}
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory

/**
 * Decoded metadata and data from a single GRIB message.
 */
case class DecodedGribMessage(
    messageIndex: Int,
    discipline: Int,
    variableShortName: String,
    variableName: String,
    units: String,
    levelType: String,
    levelValue: Double,
    referenceTime: Timestamp,
    forecastTime: Timestamp,
    nx: Int,
    ny: Int,
    latStart: Double,
    lonStart: Double,
    latIncrement: Double,
    lonIncrement: Double,
    lats: Array[Double],
    lons: Array[Double],
    values: Array[Float]
)

/**
 * Wraps netCDF-Java to decode GRIB1 and GRIB2 messages from files or byte buffers.
 */
object GribMessageDecoder {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Scan a local file and decode all GRIB messages.
   * Tries GRIB2 first, falls back to GRIB1.
   */
  def decodeFile(filePath: String): Seq[DecodedGribMessage] = {
    val raf = new RandomAccessFile(filePath, "r")
    try {
      raf.seek(0)
      val edition = detectEdition(raf)
      raf.seek(0)

      edition match {
        case 2 => decodeGrib2(raf)
        case 1 => decodeGrib1(raf)
        case _ =>
          val g2 = decodeGrib2(raf)
          if (g2.nonEmpty) g2
          else {
            raf.seek(0)
            decodeGrib1(raf)
          }
      }
    } finally {
      raf.close()
    }
  }

  /**
   * Decode GRIB messages from a file with a known edition and starting index.
   */
  def decodeFromFile(filePath: String, edition: Int, startIndex: Int): Seq[DecodedGribMessage] = {
    val raf = new RandomAccessFile(filePath, "r")
    try {
      edition match {
        case 2 => decodeGrib2(raf, startIndex)
        case 1 => decodeGrib1(raf, startIndex)
        case _ => decodeGrib2(raf, startIndex) ++ { raf.seek(0); decodeGrib1(raf, startIndex) }
      }
    } finally {
      raf.close()
    }
  }

  /** Detect GRIB edition (1 or 2) from the file header. Returns 0 if undetermined. */
  private def detectEdition(raf: RandomAccessFile): Int = {
    try {
      val startPos = raf.getFilePointer
      val buf = new Array[Byte](16)
      raf.readFully(buf)
      raf.seek(startPos)
      val magic = new String(buf, 0, 4, "ASCII")
      if (magic == "GRIB") buf(7).toInt & 0xFF else 0
    } catch {
      case _: IOException => 0
    }
  }

  // ============================================================
  // GRIB2
  // ============================================================

  private def decodeGrib2(raf: RandomAccessFile, startIndex: Int = 0): Seq[DecodedGribMessage] = {
    val messages = ArrayBuffer.empty[DecodedGribMessage]
    val scanner = new Grib2RecordScanner(raf)
    var idx = startIndex

    while (scanner.hasNext) {
      try {
        val record = scanner.next()
        decodeGrib2Record(record, raf, idx).foreach(messages += _)
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to decode GRIB2 message at index $idx: ${e.getMessage}")
      }
      idx += 1
    }
    messages.toSeq
  }

  private def decodeGrib2Record(
      record: Grib2Record,
      raf: RandomAccessFile,
      messageIndex: Int
  ): Option[DecodedGribMessage] = {
    try {
      val is = record.getId
      val pds = record.getPDS
      val gds = record.getGDS
      val discipline = record.getDiscipline

      // Look up parameter using Grib2Tables
      val tables = Grib2Tables.factory(
        is.getCenter_id, is.getSubcenter_id,
        is.getMaster_table_version, is.getLocal_table_version, -1)
      val param = tables.getParameter(discipline, pds)
      val shortName = if (param != null && param.getAbbrev != null) param.getAbbrev
                      else s"var${discipline}_${pds.getParameterCategory}_${pds.getParameterNumber}"
      val longName = if (param != null && param.getName != null) param.getName else shortName
      val units = if (param != null && param.getUnit != null) param.getUnit else "unknown"

      // Level
      val levelType1 = pds.getLevelType1
      val levelTypeName = tables.getLevelName(levelType1)
      val levelValue = pds.getLevelValue1

      // Reference time
      val refCalDate = is.getReferenceDate
      val referenceTimestamp = new Timestamp(refCalDate.getMillis)

      // Forecast time
      val forecastCalDate = tables.getForecastDate(record)
      val forecastTimestamp = if (forecastCalDate != null) {
        new Timestamp(forecastCalDate.getMillis)
      } else {
        referenceTimestamp
      }

      // Grid geometry via GdsHorizCoordSys
      val nx = gds.getNx
      val ny = gds.getNy
      val hcs = gds.makeHorizCoordSys()

      val latStart = hcs.starty
      val lonStart = hcs.startx
      val latInc = Math.abs(hcs.dy)
      val lonInc = Math.abs(hcs.dx)

      // Build lat/lon arrays
      val (lats, lons) = buildLatLonFromHcs(hcs, nx, ny)

      // Decode data
      val dataValues = record.readData(raf)

      Some(DecodedGribMessage(
        messageIndex = messageIndex,
        discipline = discipline,
        variableShortName = shortName,
        variableName = longName,
        units = units,
        levelType = if (levelTypeName != null) levelTypeName else s"level_$levelType1",
        levelValue = levelValue,
        referenceTime = referenceTimestamp,
        forecastTime = forecastTimestamp,
        nx = nx,
        ny = ny,
        latStart = latStart,
        lonStart = lonStart,
        latIncrement = latInc,
        lonIncrement = lonInc,
        lats = lats,
        lons = lons,
        values = dataValues
      ))
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to decode GRIB2 record $messageIndex: ${e.getMessage}", e)
        None
    }
  }

  // ============================================================
  // GRIB1
  // ============================================================

  private def decodeGrib1(raf: RandomAccessFile, startIndex: Int = 0): Seq[DecodedGribMessage] = {
    val messages = ArrayBuffer.empty[DecodedGribMessage]
    val scanner = new Grib1RecordScanner(raf)
    var idx = startIndex

    while (scanner.hasNext) {
      try {
        val record = scanner.next()
        decodeGrib1Record(record, raf, idx).foreach(messages += _)
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to decode GRIB1 message at index $idx: ${e.getMessage}")
      }
      idx += 1
    }
    messages.toSeq
  }

  private def decodeGrib1Record(
      record: Grib1Record,
      raf: RandomAccessFile,
      messageIndex: Int
  ): Option[DecodedGribMessage] = {
    try {
      val pds = record.getPDSsection
      val gds = record.getGDS // Grib1Gds

      val center = pds.getCenter
      val subCenter = pds.getSubCenter
      val tableVersion = pds.getTableVersion
      val paramNumber = pds.getParameterNumber

      // Look up parameter via Grib1Customizer
      val customizer = Grib1Customizer.factory(center, subCenter, tableVersion, new Grib1ParamTables())
      val param = customizer.getParameter(center, subCenter, tableVersion, paramNumber)
      val shortName = if (param != null && param.getAbbrev != null) param.getAbbrev else s"var_$paramNumber"
      val longName = if (param != null && param.getName != null) param.getName else shortName
      val units = if (param != null && param.getUnit != null) param.getUnit else "unknown"

      // Level
      val levelType = pds.getLevelType
      val levelTypeName = customizer.getLevelDescription(levelType)
      val levelValue = pds.getLevelValue1.toDouble

      // Reference time
      val refCalDate = pds.getReferenceDate
      val referenceTimestamp = new Timestamp(refCalDate.getMillis)

      // Forecast time — reference time + forecast offset (in hours)
      val forecastTimestamp = {
        try {
          val paramTime = customizer.getParamTime(pds)
          if (paramTime != null) {
            val fcstHours = paramTime.getForecastTime
            new Timestamp(refCalDate.getMillis + fcstHours * 3600L * 1000L)
          } else {
            referenceTimestamp
          }
        } catch {
          case _: Exception => referenceTimestamp
        }
      }

      // Grid geometry
      val nx = gds.getNx
      val ny = gds.getNy
      val hcs = gds.makeHorizCoordSys()

      val latStart = hcs.starty
      val lonStart = hcs.startx
      val latInc = Math.abs(hcs.dy)
      val lonInc = Math.abs(hcs.dx)

      val (lats, lons) = buildLatLonFromHcs(hcs, nx, ny)

      // Decode data
      val data = record.readData(raf)

      Some(DecodedGribMessage(
        messageIndex = messageIndex,
        discipline = 0, // GRIB1 is always meteorological
        variableShortName = shortName,
        variableName = longName,
        units = units,
        levelType = if (levelTypeName != null) levelTypeName else s"level_$levelType",
        levelValue = levelValue,
        referenceTime = referenceTimestamp,
        forecastTime = forecastTimestamp,
        nx = nx,
        ny = ny,
        latStart = latStart,
        lonStart = lonStart,
        latIncrement = latInc,
        lonIncrement = lonInc,
        lats = lats,
        lons = lons,
        values = data
      ))
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to decode GRIB1 record $messageIndex: ${e.getMessage}", e)
        None
    }
  }

  // ============================================================
  // Shared helpers
  // ============================================================

  /**
   * Build lat/lon arrays from a GdsHorizCoordSys.
   * For lat/lon grids, startx/starty/dx/dy are in degrees.
   */
  private def buildLatLonFromHcs(
      hcs: GdsHorizCoordSys,
      nx: Int,
      ny: Int
  ): (Array[Double], Array[Double]) = {
    val nPoints = ny * nx
    val lats = new Array[Double](nPoints)
    val lons = new Array[Double](nPoints)

    if (hcs.isLatLon) {
      // Direct lat/lon grid
      val latStart = hcs.starty
      val lonStart = hcs.startx
      val dy = hcs.dy // may be negative (N→S scan)
      val dx = hcs.dx

      var i = 0
      var y = 0
      while (y < ny) {
        val lat = latStart + y * dy
        var x = 0
        while (x < nx) {
          var lon = lonStart + x * dx
          // Normalise to 0–360
          while (lon < 0) lon += 360.0
          while (lon >= 360.0) lon -= 360.0
          lats(i) = lat
          lons(i) = lon
          i += 1
          x += 1
        }
        y += 1
      }
    } else {
      // Projected grid — convert via projection
      val proj = hcs.proj
      val startX = hcs.startx
      val startY = hcs.starty
      val dx = hcs.dx
      val dy = hcs.dy

      var i = 0
      var y = 0
      while (y < ny) {
        val py = startY + y * dy
        var x = 0
        while (x < nx) {
          val px = startX + x * dx
          val projPoint = new ucar.unidata.geoloc.ProjectionPointImpl(px, py)
          val latLon = proj.projToLatLon(projPoint)
          lats(i) = latLon.getLatitude
          var lon = latLon.getLongitude
          while (lon < 0) lon += 360.0
          while (lon >= 360.0) lon -= 360.0
          lons(i) = lon
          i += 1
          x += 1
        }
        y += 1
      }
    }

    (lats, lons)
  }
}
