package com.example.spark.grib.tools

import com.example.spark.grib.{DecodedGribMessage, GribMessageDecoder}
import java.io.{BufferedOutputStream, File, FileOutputStream, PrintWriter}
import java.nio.{ByteBuffer, ByteOrder}

/**
 * Dumps what the netCDF-Java decoder produces for a GRIB file, as a reference
 * for the Rust spike's parity check (`rust/scripts/parity.py`):
 *
 *  - `<out>/<name>.jsonl`: one JSON object of metadata per message
 *  - `<out>/<name>.f32`:   every message's values, little-endian float32,
 *                          concatenated in message order
 *
 * Usage: `sbt "Test/runMain com.example.spark.grib.tools.DumpDecodedMessages <grib> <outDir>"`
 */
object DumpDecodedMessages {

  def main(args: Array[String]): Unit = {
    require(args.length == 2, "usage: DumpDecodedMessages <grib-file> <out-dir>")
    val (input, outDir) = (new File(args(0)), new File(args(1)))
    outDir.mkdirs()

    val it = GribMessageDecoder.openMessages(input.getPath, computeLatLon = false)
    val metadata = new PrintWriter(new File(outDir, s"${input.getName}.jsonl"), "UTF-8")
    val values = new BufferedOutputStream(new FileOutputStream(new File(outDir, s"${input.getName}.f32")))
    try {
      it.foreach { msg =>
        metadata.println(toJson(msg))
        values.write(littleEndian(msg.values))
      }
    } finally {
      it.close()
      metadata.close()
      values.close()
    }
  }

  private def toJson(msg: DecodedGribMessage): String = {
    def str(s: String) = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    def num(d: Double) = if (d.isNaN || d.isInfinite) "null" else d.toString
    Seq(
      "message_index"       -> msg.messageIndex.toString,
      "discipline"          -> msg.discipline.toString,
      "variable_short_name" -> str(msg.variableShortName),
      "variable_name"       -> str(msg.variableName),
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
      "num_values"          -> msg.values.length.toString
    ).map { case (k, v) => s"${str(k)}:$v" }.mkString("{", ",", "}")
  }

  private def littleEndian(values: Array[Float]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(values.length * java.lang.Float.BYTES).order(ByteOrder.LITTLE_ENDIAN)
    buffer.asFloatBuffer().put(values)
    buffer.array()
  }
}
