package com.example.spark.grib.tools

import com.example.spark.grib.{DecodedGribMessage, GribMessageDecoder, ProductAttributes}
import java.io.{BufferedOutputStream, File, FileOutputStream, PrintWriter}
import java.nio.{ByteBuffer, ByteOrder}

/**
 * Dumps what the netCDF-Java decoder produces for a GRIB file, as a reference
 * for the Rust spike's parity check (`rust/scripts/parity.py`):
 *
 *  - `<out>/<name>.jsonl`: one JSON object of metadata per message
 *  - `<out>/<name>.f32`:   every message's values, little-endian float32,
 *                          concatenated in message order
 *  - `<out>/<name>.latlon.f64`: the first message's latitudes then longitudes
 *                          (flat mode), little-endian float64
 *
 * Usage: `sbt "Test/runMain com.example.spark.grib.tools.DumpDecodedMessages <grib> <outDir>"`
 */
object DumpDecodedMessages {

  def main(args: Array[String]): Unit = {
    require(args.length == 2, "usage: DumpDecodedMessages <grib-file> <out-dir>")
    val (input, outDir) = (new File(args(0)), new File(args(1)))
    outDir.mkdirs()

    val it = GribMessageDecoder.openMessages(input.getPath, computeLatLon = true)
    val metadata = new PrintWriter(new File(outDir, s"${input.getName}.jsonl"), "UTF-8")
    val values = new BufferedOutputStream(new FileOutputStream(new File(outDir, s"${input.getName}.f32")))
    try {
      it.zipWithIndex.foreach { case (msg, i) =>
        metadata.println(toJson(msg))
        values.write(littleEndian(msg.values))
        if (i == 0) writeLatLon(new File(outDir, s"${input.getName}.latlon.f64"), msg)
      }
    } finally {
      it.close()
      metadata.close()
      values.close()
    }
  }

  private def writeLatLon(file: File, msg: DecodedGribMessage): Unit = {
    val points = msg.lats ++ msg.lons
    val buffer = ByteBuffer.allocate(points.length * java.lang.Double.BYTES).order(ByteOrder.LITTLE_ENDIAN)
    buffer.asDoubleBuffer().put(points)
    val out = new FileOutputStream(file)
    try out.write(buffer.array()) finally out.close()
  }

  private def str(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
  private def num(d: Double): String = if (d.isNaN || d.isInfinite) "null" else d.toString
  private def opt[A](o: Option[A])(f: A => String): String = o.map(f).getOrElse("null")
  private def obj(fields: (String, String)*): String = fields.map { case (k, v) => s"${str(k)}:$v" }.mkString("{", ",", "}")

  private def productJson(p: ProductAttributes): String = obj(
    "aerosol" -> opt(p.aerosol)(a => obj(
      "type" -> a.aerosolType.toString, "type_name" -> opt(a.typeName)(str),
      "size_interval_type" -> opt(a.sizeIntervalType)(_.toString),
      "size_first_m" -> opt(a.sizeFirstM)(num), "size_second_m" -> opt(a.sizeSecondM)(num),
      "wavelength_interval_type" -> opt(a.wavelengthIntervalType)(_.toString),
      "wavelength_first_m" -> opt(a.wavelengthFirstM)(num), "wavelength_second_m" -> opt(a.wavelengthSecondM)(num))),
    "ensemble" -> opt(p.ensemble)(e => obj(
      "perturbation_type" -> e.perturbationType.toString, "perturbation_type_name" -> opt(e.perturbationTypeName)(str),
      "member" -> e.member.toString, "ensemble_size" -> e.ensembleSize.toString)),
    "ensemble_derived" -> opt(p.ensembleDerived)(d => obj(
      "type" -> d.derivedType.toString, "type_name" -> opt(d.typeName)(str), "ensemble_size" -> d.ensembleSize.toString)),
    "probability" -> opt(p.probability)(pr => obj(
      "type" -> pr.probabilityType.toString, "type_name" -> opt(pr.typeName)(str),
      "lower_limit" -> opt(pr.lowerLimit)(num), "upper_limit" -> opt(pr.upperLimit)(num))),
    "percentile" -> opt(p.percentile)(_.toString),
    "statistic" -> opt(p.statistic)(s => obj(
      "process" -> s.process.toString, "process_name" -> opt(s.processName)(str),
      "interval_hours" -> opt(s.intervalHours)(num)))
  )

  private def toJson(msg: DecodedGribMessage): String = {
    Seq(
      "message_index"       -> msg.messageIndex.toString,
      "discipline"          -> msg.discipline.toString,
      "variable_short_name" -> str(msg.variableShortName),
      "variable_name"       -> str(msg.variableName),
      "units"               -> str(msg.units),
      "level_type"          -> str(msg.levelType),
      "level_value"         -> num(msg.levelValue),
      "level_value2"        -> msg.levelValue2.map(num).getOrElse("null"),
      "reference_time_ms"   -> msg.referenceTime.getTime.toString,
      "forecast_time_ms"    -> msg.forecastTime.getTime.toString,
      "pds_template"        -> msg.pdsTemplate.map(_.toString).getOrElse("null"),
      "nx"                  -> msg.nx.toString,
      "ny"                  -> msg.ny.toString,
      "lat_start"           -> num(msg.latStart),
      "lon_start"           -> num(msg.lonStart),
      "lat_increment"       -> num(msg.latIncrement),
      "lon_increment"       -> num(msg.lonIncrement),
      "num_values"          -> msg.values.length.toString,
      "product"             -> productJson(msg.product)
    ).map { case (k, v) => s"${str(k)}:$v" }.mkString("{", ",", "}")
  }

  private def littleEndian(values: Array[Float]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(values.length * java.lang.Float.BYTES).order(ByteOrder.LITTLE_ENDIAN)
    buffer.asFloatBuffer().put(values)
    buffer.array()
  }
}
