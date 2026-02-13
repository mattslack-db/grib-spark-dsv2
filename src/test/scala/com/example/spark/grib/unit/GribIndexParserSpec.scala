package com.example.spark.grib.unit

import com.example.spark.grib.{GribIndexEntry, GribIndexParser}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GribIndexParserSpec extends AnyFunSuite with Matchers {

  val sampleIdx: String =
    """1:0:d=2024010100:HGT:1 mb:anl:
      |2:83345:d=2024010100:TMP:1 mb:anl:
      |3:152498:d=2024010100:RH:1 mb:anl:
      |4:201234:d=2024010100:UGRD:1 mb:anl:
      |5:270000:d=2024010100:VGRD:1 mb:anl:""".stripMargin

  test("parse should extract correct number of entries") {
    val entries = GribIndexParser.parse(sampleIdx)
    entries should have length 5
  }

  test("parse should correctly extract fields from each line") {
    val entries = GribIndexParser.parse(sampleIdx)

    entries.head shouldBe GribIndexEntry(
      messageNumber = 1,
      byteOffset = 0L,
      referenceTime = "2024010100",
      variableName = "HGT",
      levelDescription = "1 mb",
      forecastInfo = "anl"
    )

    entries(1).messageNumber shouldBe 2
    entries(1).byteOffset shouldBe 83345L
    entries(1).variableName shouldBe "TMP"
  }

  test("parse should handle empty input") {
    GribIndexParser.parse("") shouldBe empty
    GribIndexParser.parse(null) shouldBe empty
  }

  test("parse should skip malformed lines") {
    val input =
      """1:0:d=2024010100:HGT:1 mb:anl:
        |this is garbage
        |3:152498:d=2024010100:RH:1 mb:anl:""".stripMargin
    val entries = GribIndexParser.parse(input)
    entries should have length 2
    entries.head.variableName shouldBe "HGT"
    entries(1).variableName shouldBe "RH"
  }

  test("parse should handle lines with extra colon-separated fields") {
    val input = "1:0:d=2024010100:HGT:1 mb:anl:extra:fields:"
    val entries = GribIndexParser.parse(input)
    entries should have length 1
    entries.head.variableName shouldBe "HGT"
  }

  test("computeMessageRanges should calculate correct byte ranges") {
    val entries = GribIndexParser.parse(sampleIdx)
    val fileSize = 350000L
    val ranges = GribIndexParser.computeMessageRanges(entries, fileSize)

    ranges should have length 5

    // First message: offset 0, length = 83345 - 0
    ranges(0) shouldBe (0L, 83345L)

    // Second message: offset 83345, length = 152498 - 83345
    ranges(1) shouldBe (83345L, 152498L - 83345L)

    // Last message: extends to EOF
    ranges(4) shouldBe (270000L, 350000L - 270000L)
  }

  test("computeMessageRanges should handle single entry") {
    val entries = Seq(GribIndexEntry(1, 0L, "2024010100", "TMP", "surface", "anl"))
    val ranges = GribIndexParser.computeMessageRanges(entries, 100000L)
    ranges should have length 1
    ranges.head shouldBe (0L, 100000L)
  }

  test("computeMessageRanges should handle empty entries") {
    GribIndexParser.computeMessageRanges(Seq.empty, 100000L) shouldBe empty
  }

  test("filterByVariables should keep only matching variables") {
    val entries = GribIndexParser.parse(sampleIdx)

    val filtered = GribIndexParser.filterByVariables(entries, Set("TMP", "RH"))
    filtered should have length 2
    filtered.map(_.variableName) should contain theSameElementsAs Seq("TMP", "RH")
  }

  test("filterByVariables with empty set should keep all entries") {
    val entries = GribIndexParser.parse(sampleIdx)
    val filtered = GribIndexParser.filterByVariables(entries, Set.empty)
    filtered should have length 5
  }

  test("filterByLevelType should filter by level description") {
    val entries = Seq(
      GribIndexEntry(1, 0, "2024010100", "TMP", "surface", "anl"),
      GribIndexEntry(2, 100, "2024010100", "TMP", "1 mb", "anl"),
      GribIndexEntry(3, 200, "2024010100", "TMP", "2 m above ground", "anl")
    )

    val filtered = GribIndexParser.filterByLevelType(entries, Some("surface"))
    filtered should have length 1
    filtered.head.levelDescription shouldBe "surface"
  }

  test("filterByLevelType should be case-insensitive") {
    val entries = Seq(
      GribIndexEntry(1, 0, "2024010100", "TMP", "Surface", "anl"),
      GribIndexEntry(2, 100, "2024010100", "TMP", "1 mb", "anl")
    )
    val filtered = GribIndexParser.filterByLevelType(entries, Some("surface"))
    filtered should have length 1
  }

  test("filterByLevelType with None should keep all entries") {
    val entries = GribIndexParser.parse(sampleIdx)
    val filtered = GribIndexParser.filterByLevelType(entries, None)
    filtered should have length 5
  }
}
