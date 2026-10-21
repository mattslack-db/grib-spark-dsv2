package com.example.spark.grib.unit

import com.example.spark.grib.{GribIndexEntry, GribIndexParser, MessageRange}
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

  // UGRD and VGRD packed into one GRIB message: two .idx lines, one byte offset.
  val subMessageIdx: String =
    """1:0:d=2024010100:HGT:100 mb:anl:
      |2:5000:d=2024010100:TMP:100 mb:anl:
      |3.1:9000:d=2024010100:UGRD:100 mb:anl:
      |3.2:9000:d=2024010100:VGRD:100 mb:anl:
      |4:15000:d=2024010100:HGT:125 mb:anl:""".stripMargin

  test("parse keeps sub-message lines and records the sub-message number") {
    val entries = GribIndexParser.parse(subMessageIdx)

    entries should have length 5
    entries.map(e => (e.messageNumber, e.subMessage)) shouldBe Seq(
      (1, None), (2, None), (3, Some(1)), (3, Some(2)), (4, None))
    entries(3).variableName shouldBe "VGRD"
    entries(3).byteOffset shouldBe 9000L
  }

  test("computeMessageRanges gives sub-messages their whole GRIB message") {
    val ranges = GribIndexParser.computeMessageRanges(GribIndexParser.parse(subMessageIdx), 20000L)

    ranges(2) shouldBe (9000L, 6000L)
    ranges(3) shouldBe (9000L, 6000L)
    ranges(1) shouldBe (5000L, 4000L)
  }

  test("groupMessages yields one entry per physical GRIB message, numbered by field position") {
    val messages = GribIndexParser.groupMessages(GribIndexParser.parse(subMessageIdx), 20000L)

    messages.map(_.range) shouldBe Seq(
      MessageRange(0, 0L, 5000L),
      MessageRange(1, 5000L, 4000L),
      MessageRange(2, 9000L, 6000L), // decodes to fields 2 (UGRD) and 3 (VGRD)
      MessageRange(4, 15000L, 5000L)
    )
    messages(2).entries.map(_.variableName) shouldBe Seq("UGRD", "VGRD")
  }

  test("parseStrict returns every entry when all lines parse, ignoring blank lines") {
    GribIndexParser.parseStrict(sampleIdx + "\n\n").map(_.length) shouldBe Some(5)
    GribIndexParser.parseStrict(subMessageIdx).map(_.length) shouldBe Some(5)
  }

  test("parseStrict rejects the whole index if any line is malformed") {
    // A skipped line would silently merge its message into the previous entry's byte range.
    GribIndexParser.parseStrict(sampleIdx + "\nthis is garbage") shouldBe None
    GribIndexParser.parseStrict("1:0:d=2024010100:HGT:1 mb:anl:\n12.a:500:d=2024010100:TMP:1 mb:anl:") shouldBe None
  }
}

