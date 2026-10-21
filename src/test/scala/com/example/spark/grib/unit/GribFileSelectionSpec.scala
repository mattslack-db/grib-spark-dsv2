package com.example.spark.grib.unit

import com.example.spark.grib.GribScan
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GribFileSelectionSpec extends AnyFunSuite with Matchers {

  test("picks up GRIB files with conventional extensions") {
    Seq("a.grib", "a.grib2", "a.grb", "a.grb2", "A.GRIB2").foreach { name =>
      GribScan.isGribFileCandidate(name) shouldBe true
    }
  }

  test("picks up NOAA file names, which use dots but no GRIB extension") {
    Seq(
      "gfs.t00z.pgrb2.1p00.f000",
      "gep01.t00z.pgrb2a.0p50.f006",
      "rap.t00z.awp130pgrbf00.grib2",
      "rtma2p5.t00z.2dvaranl_ndfd.grb2_wexp",
      "YAUZ98_KWBN_202401010000",
      "gfsdata"
    ).foreach { name =>
      GribScan.isGribFileCandidate(name) shouldBe true
    }
  }

  test("skips sidecars, checksums, netCDF-Java indexes and partial downloads") {
    Seq(
      "gfs.t00z.pgrb2.1p00.f000.idx",
      "a.grib2.IDX",
      "a.grib2.md5",
      "a.grib2.sha256",
      "a.grib2.gbx9",
      "a.grib2.ncx4",
      "a.grib2.part",
      "a.grib2.tmp",
      "README.txt",
      "manifest.json"
    ).foreach { name =>
      GribScan.isGribFileCandidate(name) shouldBe false
    }
  }

  test("skips hidden files and Spark/Hadoop markers") {
    Seq(".a.grib2.crc", "_SUCCESS", "_committed_123", ".DS_Store").foreach { name =>
      GribScan.isGribFileCandidate(name) shouldBe false
    }
  }

  test("skips documents, tables, archives, images, scripts and other data formats kept next to GRIB files") {
    Seq(
      "README.md", "notes.MD", "manifest.csv", "stations.tsv", "config.yaml", "config.yml",
      "data.nc", "data.nc4", "data.h5", "data.hdf", "data.parquet",
      "gfs.f000.grib2.gz", "bundle.zip", "bundle.tar", "a.bz2", "a.xz", "a.zst",
      "plot.png", "plot.jpg", "plot.jpeg", "plot.gif", "report.pdf",
      "fetch.py", "fetch.sh", "catalog.ncml"
    ).foreach { name =>
      withClue(name) { GribScan.isGribFileCandidate(name) shouldBe false }
    }
  }
}


class GribScanParallelSpec extends AnyFunSuite with Matchers {

  test("inParallel keeps the input order") {
    GribScan.inParallel((1 to 50).toVector, threads = 8) { i => Thread.sleep(50 - i); i * 2 } shouldBe
      (1 to 50).map(_ * 2)
  }

  test("inParallel rethrows a failure") {
    an[IllegalStateException] should be thrownBy
      GribScan.inParallel(Seq(1, 2, 3), threads = 2) { i => if (i == 2) throw new IllegalStateException("boom") else i }
  }
}
