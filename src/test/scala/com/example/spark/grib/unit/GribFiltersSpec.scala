package com.example.spark.grib.unit

import com.example.spark.grib.{DecodedGribMessage, GribFilters, GribIndexEntry}
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

    // Not evaluable → Spark must apply them post-scan.
    GribFilters.isSupported(EqualTo("level_value", 500.0)) shouldBe false
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

  // --- matchesAnyVariable ---

  test("matchesAnyVariable is case-insensitive and treats empty as keep-all") {
    GribFilters.matchesAnyVariable("TMP", Set.empty) shouldBe true
    GribFilters.matchesAnyVariable("TMP", Set("tmp", "rh")) shouldBe true
    GribFilters.matchesAnyVariable("UGRD", Set("tmp", "rh")) shouldBe false
  }
}
