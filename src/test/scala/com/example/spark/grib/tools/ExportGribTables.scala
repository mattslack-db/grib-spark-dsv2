package com.example.spark.grib.tools

import com.example.spark.grib.{AerosolTypeNames, ParameterUnits}
import java.io.{File, PrintWriter}
import ucar.nc2.grib.GribTables
import ucar.nc2.grib.grib2.table.Grib2Tables

/**
 * Exports the GRIB2 tables the Scala reader takes from netCDF-Java (with this
 * repo's corrections applied) as TSV, so the Rust reader in `rust/` names
 * parameters, units and levels identically:
 *
 *  - `params.tsv`: table, discipline, category, number, abbreviation, name, unit
 *  - `codes.tsv`:  table, code table (4.5, 4.6, 4.7, 4.9, 4.10, 4.233), code, name
 *
 * `table` is `wmo` or `ncep`: the Rust reader uses `ncep` for centre 7 and
 * `wmo` otherwise. Other centres' local tables are not exported.
 *
 * Usage: `sbt "Test/runMain com.example.spark.grib.tools.ExportGribTables rust/data"`
 */
object ExportGribTables {

  private val NcepCentre = 7
  // Table versions the NOAA files in this repo's fixtures use; netCDF-Java
  // resolves local tables by centre, so the versions don't change the content.
  private val Tables = Seq(
    "wmo"  -> Grib2Tables.factory(0, 0, 2, 0, -1),
    "ncep" -> Grib2Tables.factory(NcepCentre, 0, 2, 1, -1)
  )
  private val CodeRange = 0 to 255
  private val AerosolCodeRange = 0 to 65535

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "usage: ExportGribTables <out-dir>")
    val outDir = new File(args(0))
    outDir.mkdirs()
    writeTsv(new File(outDir, "params.tsv"), Seq("table", "discipline", "category", "number", "abbrev", "name", "unit"), paramRows)
    writeTsv(new File(outDir, "codes.tsv"), Seq("table", "code_table", "code", "name"), codeRows)
  }

  // getParameters() can't be used: NcepLocalTables lists its resources from a
  // jar it has already closed. getParameter(discipline, pds), which the reader
  // calls, delegates to this package-private lookup, so every code is probed.
  private val parameterLookup = {
    val method = classOf[Grib2Tables].getDeclaredMethod("getParameter", classOf[Int], classOf[Int], classOf[Int])
    method.setAccessible(true)
    method
  }

  private def parameter(tables: Grib2Tables, d: Int, c: Int, n: Int): Option[GribTables.Parameter] =
    Option(parameterLookup.invoke(tables, Int.box(d), Int.box(c), Int.box(n)).asInstanceOf[GribTables.Parameter])

  private def paramRows: Seq[Seq[Any]] =
    for {
      (table, tables) <- Tables
      centre = if (table == "ncep") NcepCentre else 0
      d <- CodeRange
      c <- CodeRange
      n <- CodeRange
      p <- parameter(tables, d, c, n)
    } yield Seq(
      table, p.getDiscipline, p.getCategory, p.getNumber,
      Option(p.getAbbrev).getOrElse(""), Option(p.getName).getOrElse(""),
      ParameterUnits.resolve(centre, p.getDiscipline, p.getCategory, p.getNumber, Option(p.getUnit).getOrElse("unknown"))
    )

  private def codeRows: Seq[Seq[Any]] = Tables.flatMap { case (table, tables) =>
    val lookups: Seq[(String, Range, Int => String)] = Seq(
      ("4.5", CodeRange, tables.getLevelName _),
      ("4.6", CodeRange, tables.getCodeTableValue("4.6", _)),
      ("4.7", CodeRange, tables.getCodeTableValue("4.7", _)),
      ("4.9", CodeRange, tables.getCodeTableValue("4.9", _)),
      ("4.10", CodeRange, tables.getStatisticName _),
      ("4.233", AerosolCodeRange, AerosolTypeNames.lookup(_, tables).orNull)
    )
    for {
      (codeTable, codes, lookup) <- lookups
      code <- codes
      name <- Option(lookup(code)).filter(_.nonEmpty)
    } yield Seq(table, codeTable, code, name)
  }

  private def writeTsv(file: File, header: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    val out = new PrintWriter(file, "UTF-8")
    try {
      out.println(header.mkString("\t"))
      rows.foreach(r => out.println(r.map(v => v.toString.replaceAll("[\t\n\r]", " ")).mkString("\t")))
    } finally out.close()
  }
}
