package com.example.spark.grib

import java.io.{File, FileOutputStream, InputStream, OutputStream}

/** Small I/O helpers shared by planning, reading and decoding. */
object GribIo {

  private val CopyBufferBytes = 1024 * 1024

  /** Bytes of a GRIB message's start that identify it: "GRIB", 2 reserved, discipline, edition. */
  val HeaderBytes = 8

  /** The GRIB edition (1 or 2) from a message's first [[HeaderBytes]] bytes, or 0 if they aren't a GRIB header. */
  def edition(header: Array[Byte]): Int =
    if (header.length >= HeaderBytes && new String(header, 0, 4, "US-ASCII") == "GRIB") header(HeaderBytes - 1) & 0xFF
    else 0

  /** Copy `length` bytes of `in` (or up to EOF, if it's shorter) to `out`. */
  def copy(in: InputStream, out: OutputStream, length: Long = Long.MaxValue): Unit = {
    val buf = new Array[Byte](CopyBufferBytes)
    var remaining = length
    var n = 0
    while (remaining > 0 && { n = in.read(buf, 0, math.min(buf.length.toLong, remaining).toInt); n > 0 }) {
      out.write(buf, 0, n)
      remaining -= n
    }
  }

  /**
   * Copy `length` bytes of `in` (or up to EOF) to a new local temp file. The
   * caller owns the file and must delete it.
   */
  def copyToTempFile(in: InputStream, prefix: String, length: Long = Long.MaxValue): File = {
    val file = File.createTempFile(prefix, ".tmp")
    file.deleteOnExit()
    try {
      val out = new FileOutputStream(file)
      try copy(in, out, length) finally out.close()
      file
    } catch {
      case e: Throwable =>
        file.delete()
        throw e
    }
  }
}
