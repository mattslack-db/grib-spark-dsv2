package com.example.spark.grib

import ucar.nc2.grib.grib2.{Grib2RecordScanner, Grib2Record, Grib2Gds}
import ucar.nc2.grib.grib2.Grib2SectionIdentification
import ucar.nc2.grib.grib2.table.Grib2Tables
import ucar.nc2.grib.grib1.{Grib1RecordScanner, Grib1Record, Grib1Gds}
import ucar.nc2.grib.grib1.tables.{Grib1Customizer, Grib1ParamTables}
import ucar.nc2.grib.GdsHorizCoordSys
import ucar.unidata.io.RandomAccessFile
import java.io.{Closeable, IOException}
import java.sql.Timestamp
import java.util.{Calendar, GregorianCalendar, TimeZone}
import org.slf4j.LoggerFactory

/**
 * An iterator that owns a resource (an open file handle) which must be released
 * by calling `close()` when iteration finishes or is abandoned early.
 */
trait CloseableIterator[A] extends Iterator[A] with Closeable

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
   * Scan a local file and decode all GRIB messages, auto-detecting the edition
   * from the file header (tries GRIB2 first, falls back to GRIB1).
   *
   * @param filePath     Local path to a GRIB file, or a byte-range slice that
   *                     begins on a message boundary.
   * @param startIndex   Ordinal to assign to the first decoded message. Used
   *                     when decoding a partition slice so message indices
   *                     remain globally consistent across slices.
   * @param computeLatLon Whether to materialize per-point lat/lon arrays. Only
   *                     flat output mode needs them; gridded mode emits the grid
   *                     origin/increments instead, so skipping the arrays there
   *                     avoids allocating two nx*ny Double arrays per message.
   */
  def decodeFile(
      filePath: String,
      startIndex: Int = 0,
      computeLatLon: Boolean = true
  ): Seq[DecodedGribMessage] = {
    val it = openMessages(filePath, startIndex, computeLatLon)
    try it.toList finally it.close()
  }

  /**
   * Open a GRIB file (or a byte-range slice that begins on a message boundary)
   * and return a lazy, closeable iterator over its decoded messages, with the
   * edition auto-detected.
   *
   * Messages are decoded on demand, so only one is held in memory at a time.
   * The caller MUST call `close()` on the returned iterator (whether or not it
   * was fully consumed) to release the underlying file handle.
   */
  def openMessages(
      filePath: String,
      startIndex: Int = 0,
      computeLatLon: Boolean = true
  ): CloseableIterator[DecodedGribMessage] = {
    val raf = new RandomAccessFile(filePath, "r")
    try {
      raf.seek(0)
      val edition = detectEdition(raf)
      raf.seek(0)

      edition match {
        case 2 => new Grib2MessageIterator(raf, startIndex, computeLatLon)
        case 1 => new Grib1MessageIterator(raf, startIndex, computeLatLon)
        case _ =>
          // Ambiguous header: prefer GRIB2 if it yields anything, else GRIB1.
          val g2 = new Grib2MessageIterator(raf, startIndex, computeLatLon)
          if (g2.hasNext) g2
          else {
            raf.seek(0)
            new Grib1MessageIterator(raf, startIndex, computeLatLon)
          }
      }
    } catch {
      case e: Throwable =>
        raf.close()
        throw e
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

  /**
   * Base for the edition-specific message iterators. Prefetches the next
   * decodable message — skipping records that fail to decode, exactly as the
   * former eager loop did — and closes the file handle on `close()`.
   */
  private abstract class MessageIterator(
      raf: RandomAccessFile,
      startIndex: Int
  ) extends CloseableIterator[DecodedGribMessage] {

    /** Whether the underlying record scanner has another record. */
    protected def hasNextRecord: Boolean

    /** Advance the scanner by one record and decode it at the given index. */
    protected def decodeNext(index: Int): Option[DecodedGribMessage]

    private var idx = startIndex
    private var prefetched: Option[DecodedGribMessage] = None
    private var exhausted = false

    private def prefetch(): Unit = {
      while (prefetched.isEmpty && !exhausted) {
        if (!hasNextRecord) {
          exhausted = true
        } else {
          prefetched =
            try decodeNext(idx)
            catch {
              case e: Exception =>
                logger.warn(s"Failed to decode message at index $idx: ${e.getMessage}")
                None
            }
          idx += 1
        }
      }
    }

    override def hasNext: Boolean = {
      prefetch()
      prefetched.isDefined
    }

    override def next(): DecodedGribMessage = {
      prefetch()
      val msg = prefetched.getOrElse(
        throw new java.util.NoSuchElementException("next() on an exhausted GRIB iterator"))
      prefetched = None
      msg
    }

    override def close(): Unit = raf.close()
  }

  // ============================================================
  // GRIB2
  // ============================================================

  private class Grib2MessageIterator(
      raf: RandomAccessFile,
      startIndex: Int,
      computeLatLon: Boolean
  ) extends MessageIterator(raf, startIndex) {
    private val scanner = new Grib2RecordScanner(raf)
    override protected def hasNextRecord: Boolean = scanner.hasNext
    override protected def decodeNext(index: Int): Option[DecodedGribMessage] =
      decodeGrib2Record(scanner.next(), raf, index, computeLatLon)
  }

  private def decodeGrib2Record(
      record: Grib2Record,
      raf: RandomAccessFile,
      messageIndex: Int,
      computeLatLon: Boolean
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

      // Build lat/lon arrays (flat mode only; gridded mode uses origin/increments)
      val (lats, lons) =
        if (computeLatLon) buildLatLonFromHcs(hcs, nx, ny)
        else (Array.empty[Double], Array.empty[Double])

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

  private class Grib1MessageIterator(
      raf: RandomAccessFile,
      startIndex: Int,
      computeLatLon: Boolean
  ) extends MessageIterator(raf, startIndex) {
    private val scanner = new Grib1RecordScanner(raf)
    override protected def hasNextRecord: Boolean = scanner.hasNext
    override protected def decodeNext(index: Int): Option[DecodedGribMessage] =
      decodeGrib1Record(scanner.next(), raf, index, computeLatLon)
  }

  private def decodeGrib1Record(
      record: Grib1Record,
      raf: RandomAccessFile,
      messageIndex: Int,
      computeLatLon: Boolean
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

      val (lats, lons) =
        if (computeLatLon) buildLatLonFromHcs(hcs, nx, ny)
        else (Array.empty[Double], Array.empty[Double])

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
