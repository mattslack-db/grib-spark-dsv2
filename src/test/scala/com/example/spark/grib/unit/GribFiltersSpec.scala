package com.example.spark.grib.unit

import com.example.spark.grib.{DecodedGribMessage, GribFilters, GribIndexEntry, IndexLevel}
import org.apache.spark.sql.sources._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class GribFiltersSpec extends AnyFunSuite with Matchers {

  private def msg(
      variableShortName: String = "TMP",
      levelType: String = "surface",
      levelValue: Double = 0.0,
      discipline: Int = 0
  ): DecodedGribMessage =
    DecodedGribMessage(
      messageIndex = 0,
      discipline = discipline,
      variableShortName = variableShortName,
      variableName = variableShortName,
      units = "K",
      levelType = levelType,
      levelValue = levelValue,
      referenceTime = new Timestamp(0L),
      forecastTime = new Timestamp(0L),
      nx = 1,
      ny = 1,
      latStart = 0.0,
      lonStart = 0.0,
      latIncrement = 1.0,
      lonIncrement = 1.0,
      lats = Array(0.0),
      lons = Array(0.0),
      values = Array(1.0f)
    )

  private def entry(variableName: String, levelDescription: String = "surface"): GribIndexEntry =
    GribIndexEntry(1, 0L, "2024010100", variableName, levelDescription, "anl")

  // --- isSupported / passes must agree on the supported set ---

  test("isSupported accepts exactly the filters passes can evaluate") {
    GribFilters.isSupported(EqualTo("variable_short_name", "TMP")) shouldBe true
    GribFilters.isSupported(In("variable_short_name", Array("TMP", "RH"))) shouldBe true
    GribFilters.isSupported(EqualTo("level_type", "surface")) shouldBe true
    GribFilters.isSupported(EqualTo("discipline", 0)) shouldBe true
    GribFilters.isSupported(GreaterThan("level_value", 500.0)) shouldBe true
    GribFilters.isSupported(EqualTo("level_value", 500.0)) shouldBe true
    GribFilters.isSupported(In("level_value", Array(500.0, 850.0))) shouldBe true

    // Not evaluable → Spark must apply them post-scan.
    GribFilters.isSupported(GreaterThan("discipline", 0)) shouldBe false
    GribFilters.isSupported(IsNotNull("variable_short_name")) shouldBe false
    GribFilters.isSupported(GreaterThan("reference_time", new Timestamp(0L))) shouldBe false
    GribFilters.isSupported(EqualTo("latitude", 40.0)) shouldBe false
  }

  // --- passes: exact SQL semantics (finding #1 regression guard) ---

  test("passes matches variable_short_name exactly") {
    GribFilters.passes(msg(variableShortName = "TMP"), EqualTo("variable_short_name", "TMP")) shouldBe true
    GribFilters.passes(msg(variableShortName = "RH"), EqualTo("variable_short_name", "TMP")) shouldBe false
  }

  test("passes matches level_type exactly, NOT as a substring") {
    // A substring match would wrongly keep this row; equality must be exact.
    val m = msg(levelType = "depth below surface")
    GribFilters.passes(m, EqualTo("level_type", "surface")) shouldBe false
    GribFilters.passes(msg(levelType = "surface"), EqualTo("level_type", "surface")) shouldBe true
  }

  test("passes evaluates level_value range comparisons") {
    GribFilters.passes(msg(levelValue = 600.0), GreaterThan("level_value", 500.0)) shouldBe true
    GribFilters.passes(msg(levelValue = 400.0), GreaterThan("level_value", 500.0)) shouldBe false
    GribFilters.passes(msg(levelValue = 500.0), GreaterThanOrEqual("level_value", 500.0)) shouldBe true
    GribFilters.passes(msg(levelValue = 500.0), LessThan("level_value", 500.0)) shouldBe false
  }

  test("passes evaluates In and boolean compositions") {
    GribFilters.passes(msg(variableShortName = "RH"), In("variable_short_name", Array("TMP", "RH"))) shouldBe true
    GribFilters.passes(msg(variableShortName = "UGRD"), In("variable_short_name", Array("TMP", "RH"))) shouldBe false

    val and = And(EqualTo("variable_short_name", "TMP"), GreaterThan("level_value", 500.0))
    GribFilters.passes(msg(variableShortName = "TMP", levelValue = 600.0), and) shouldBe true
    GribFilters.passes(msg(variableShortName = "TMP", levelValue = 400.0), and) shouldBe false

    val not = Not(EqualTo("discipline", 0))
    GribFilters.passes(msg(discipline = 0), not) shouldBe false
    GribFilters.passes(msg(discipline = 2), not) shouldBe true
  }

  test("passes matches level_value equality and In exactly") {
    GribFilters.passes(msg(levelValue = 2.0), EqualTo("level_value", 2.0)) shouldBe true
    GribFilters.passes(msg(levelValue = 2.0), EqualTo("level_value", 2)) shouldBe true
    GribFilters.passes(msg(levelValue = 2.0), EqualTo("level_value", 10.0)) shouldBe false
    GribFilters.passes(msg(levelValue = 2.0), In("level_value", Array(10.0, 2.0))) shouldBe true
    GribFilters.passes(msg(levelValue = 2.0), In("level_value", Array(10.0))) shouldBe false
  }

  // --- indexEntryKept: a case-insensitive superset that never over-prunes ---

  test("indexEntryKept prunes variable_short_name case-insensitively") {
    GribFilters.indexEntryKept(entry("TMP"), EqualTo("variable_short_name", "tmp")) shouldBe true
    GribFilters.indexEntryKept(entry("RH"), EqualTo("variable_short_name", "TMP")) shouldBe false
    GribFilters.indexEntryKept(entry("RH"), In("variable_short_name", Array("tmp", "rh"))) shouldBe true
  }

  test("indexEntryKept keeps entries for filters it cannot resolve from .idx metadata") {
    // Level, level_value, discipline, and boolean compositions are left to the
    // authoritative message-level filter — index pruning must not drop them.
    GribFilters.indexEntryKept(entry("TMP", "500 mb"), EqualTo("level_type", "isobaricInhPa")) shouldBe true
    GribFilters.indexEntryKept(entry("TMP"), GreaterThan("level_value", 500.0)) shouldBe true
    GribFilters.indexEntryKept(entry("TMP"), Not(EqualTo("variable_short_name", "RH"))) shouldBe true
  }

  test("indexEntryKept keeps entries whose .idx name is a known alias of the decoded name") {
    // wgrib2's .idx and netCDF-Java name these parameters differently.
    GribFilters.indexEntryKept(entry("CLMR"), EqualTo("variable_short_name", "CLWMR")) shouldBe true
    GribFilters.indexEntryKept(entry("LFTX"), EqualTo("variable_short_name", "LFT X")) shouldBe true
    GribFilters.indexEntryKept(entry("CLMR"), In("variable_short_name", Array("RH", "CLWMR"))) shouldBe true
    GribFilters.indexEntryKept(entry("CLMR"), EqualTo("variable_short_name", "TMP")) shouldBe false
  }

  test("indexEntryKept never prunes on .idx names it cannot compare") {
    // A parameter without an abbreviation: wgrib2 writes its numbers, the decoder var0_16_195.
    val unnamed = entry("var discipline=0 center=7 local_table=1 parmcat=16 parm=195")
    GribFilters.indexEntryKept(unnamed, EqualTo("variable_short_name", "var0_16_195")) shouldBe true
    GribFilters.indexEntryKept(unnamed, EqualTo("variable_short_name", "TMP")) shouldBe true
    GribFilters.indexEntryKept(entry("TMP"), EqualTo("variable_short_name", "var0_16_195")) shouldBe true
  }

  test("indexEntryKept never prunes on the decoder's GRIB1 name for an unknown parameter") {
    // GRIB1 parameters missing from the tables decode as var_<number>.
    GribFilters.indexEntryKept(entry("var123"), EqualTo("variable_short_name", "var_123")) shouldBe true
    GribFilters.indexEntryKept(entry("TMP"), In("variable_short_name", Array("var_123"))) shouldBe true
  }

  test("indexEntryKept matches an alias whichever side carries the .idx spelling") {
    GribFilters.indexEntryKept(entry("CLWMR"), EqualTo("variable_short_name", "CLMR")) shouldBe true
    GribFilters.indexEntryKept(entry("CLWMR"), In("variable_short_name", Array("clmr"))) shouldBe true
    GribFilters.indexEntryKept(entry("CLWMR"), EqualTo("variable_short_name", "LFTX")) shouldBe false
  }

  test("indexEntryKept treats a NULL in an IN list as matching nothing") {
    GribFilters.indexEntryKept(entry("RH"), In("variable_short_name", Array("TMP", null))) shouldBe false
    GribFilters.indexEntryKept(entry("TMP"), In("variable_short_name", Array(null, "TMP"))) shouldBe true
  }

  test("isSupported rejects an IN list on variable_short_name containing NULL") {
    // SQL gives NULL, not false, for a non-matching row, which NOT would turn
    // into the wrong answer; Spark evaluates these itself.
    GribFilters.isSupported(In("variable_short_name", Array("TMP", null))) shouldBe false
    GribFilters.isSupported(Not(In("variable_short_name", Array("TMP", null)))) shouldBe false
  }

  // --- matchesAnyVariable ---

  test("matchesAnyVariable is case-insensitive and treats empty as keep-all") {
    GribFilters.matchesAnyVariable("TMP", Set.empty) shouldBe true
    GribFilters.matchesAnyVariable("TMP", Set("tmp", "rh")) shouldBe true
    GribFilters.matchesAnyVariable("UGRD", Set("tmp", "rh")) shouldBe false
  }

  test("matchesAnyVariable accepts a decoded name requested by its .idx alias") {
    GribFilters.matchesAnyVariable("CLWMR", Set("clmr")) shouldBe true
    GribFilters.matchesAnyVariable("LFT X", Set("LFTX")) shouldBe true
    GribFilters.matchesAnyVariable("CLWMR", Set("TMP")) shouldBe false
  }

  test("matchesAnyVariable matches an alias in both directions") {
    // If the decoder ever emitted the .idx spelling, the documented name must still work.
    GribFilters.matchesAnyVariable("CLMR", Set("CLWMR")) shouldBe true
    GribFilters.matchesAnyVariable("LFTX", Set("lft x")) shouldBe true
  }

  test("indexEntryMatchesAnyVariable keeps an .idx entry requested by either name") {
    GribFilters.indexEntryMatchesAnyVariable("CLMR", Set.empty) shouldBe true
    GribFilters.indexEntryMatchesAnyVariable("CLMR", Set("clmr")) shouldBe true
    GribFilters.indexEntryMatchesAnyVariable("CLMR", Set("CLWMR")) shouldBe true
    GribFilters.indexEntryMatchesAnyVariable("CLMR", Set("TMP")) shouldBe false
  }

  test("indexEntryMatchesAnyVariable is never stricter than matchesAnyVariable for fallback names") {
    // The variables option ignores case, so an upper-cased fallback name still finds the field.
    GribFilters.matchesAnyVariable("var0_16_195", Set("VAR0_16_195")) shouldBe true
    GribFilters.indexEntryMatchesAnyVariable("REFD", Set("VAR0_16_195")) shouldBe true
    GribFilters.indexEntryMatchesAnyVariable("REFD", Set("VAR_123")) shouldBe true
  }

  // --- indexEntryKept with a parsed .idx level ---

  private val twoMetres = Some(IndexLevel("Specified height level above ground", 2.0))
  private def keptAt2m(filter: Filter): Boolean =
    GribFilters.indexEntryKept(entry("TMP", "2 m above ground"), filter, twoMetres)

  test("indexEntryKept prunes on level when the .idx level is known") {
    keptAt2m(EqualTo("level_value", 2.0)) shouldBe true
    keptAt2m(EqualTo("level_value", 10.0)) shouldBe false
    keptAt2m(In("level_value", Array(10.0, 2.0))) shouldBe true
    keptAt2m(In("level_value", Array(10.0, 80.0))) shouldBe false
    keptAt2m(GreaterThan("level_value", 5.0)) shouldBe false
    keptAt2m(LessThanOrEqual("level_value", 2.0)) shouldBe true
    keptAt2m(EqualTo("level_type", "Specified height level above ground")) shouldBe true
    keptAt2m(EqualTo("level_type", "Isobaric surface")) shouldBe false
  }

  test("indexEntryKept combines filters with three-valued logic, pruning only on a definite false") {
    keptAt2m(And(EqualTo("variable_short_name", "TMP"), EqualTo("level_value", 10.0))) shouldBe false
    keptAt2m(Or(EqualTo("level_value", 10.0), EqualTo("variable_short_name", "TMP"))) shouldBe true
    // A level equal to the filter's (within tolerance) is only "maybe", so Not keeps it for the exact check.
    keptAt2m(Not(EqualTo("level_value", 2.0))) shouldBe true
    keptAt2m(Not(EqualTo("level_type", "Specified height level above ground"))) shouldBe false
    keptAt2m(Not(EqualTo("level_value", 10.0))) shouldBe true
    // Unknown (discipline isn't in the .idx) keeps the entry, even under Not.
    keptAt2m(Not(EqualTo("discipline", 0))) shouldBe true
    keptAt2m(And(EqualTo("discipline", 0), EqualTo("level_value", 2.0))) shouldBe true
  }

  test("indexEntryKept tolerates floating-point error in levels derived from .idx text") {
    // "0.4 mb" -> 0.4 * 100 Pa is not exactly 40.0 in floating point.
    val level = Some(IndexLevel("Isobaric surface", 0.4 * 100))
    GribFilters.indexEntryKept(entry("TMP", "0.4 mb"), EqualTo("level_value", 40.0), level) shouldBe true
    GribFilters.indexEntryKept(entry("TMP", "0.4 mb"), GreaterThanOrEqual("level_value", 40.0), level) shouldBe true
    GribFilters.indexEntryKept(entry("TMP", "0.4 mb"), LessThan("level_value", 40.0), level) shouldBe true
  }

  test("indexEntryKept never prunes on level when the .idx level is unknown") {
    GribFilters.indexEntryKept(entry("TMP", "entire atmosphere"), EqualTo("level_value", 10.0), None) shouldBe true
    GribFilters.indexEntryKept(entry("TMP", "entire atmosphere"), Not(EqualTo("level_value", 0.0)), None) shouldBe true
  }

  // --- Spark SQL semantics for NaN and -0.0, since pushed filters aren't re-checked by Spark ---

  test("passes follows Spark SQL: NaN equals NaN and is larger than any other value; -0.0 equals 0.0") {
    val nanLevel = msg(levelValue = Double.NaN)
    GribFilters.passes(nanLevel, EqualTo("level_value", Double.NaN)) shouldBe true
    GribFilters.passes(nanLevel, In("level_value", Array(1.0, Double.NaN))) shouldBe true
    GribFilters.passes(nanLevel, GreaterThan("level_value", 1e9)) shouldBe true
    GribFilters.passes(nanLevel, LessThan("level_value", 1e9)) shouldBe false
    GribFilters.passes(msg(levelValue = 5.0), LessThan("level_value", Double.NaN)) shouldBe true
    GribFilters.passes(msg(levelValue = 5.0), GreaterThanOrEqual("level_value", Double.NaN)) shouldBe false
    GribFilters.passes(msg(levelValue = -0.0), EqualTo("level_value", 0.0)) shouldBe true
  }

  test("indexEntryKept never prunes on a NaN filter value") {
    Seq[Filter](
      EqualTo("level_value", Double.NaN), LessThan("level_value", Double.NaN),
      GreaterThan("level_value", Double.NaN), Not(EqualTo("level_value", Double.NaN))
    ).foreach { f =>
      withClue(f) { keptAt2m(f) shouldBe true }
    }
  }
}

