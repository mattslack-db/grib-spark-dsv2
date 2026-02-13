package com.example.spark.grib

import org.scalatest.Tag

/**
 * Test tags for controlling which tests run in different CI configurations.
 */
object tags {
  /** Tests that require network access (S3, HTTP downloads). */
  object SlowTest extends Tag("com.example.spark.grib.tags.SlowTest")

  /** Performance/benchmark tests. */
  object PerformanceTest extends Tag("com.example.spark.grib.tags.PerformanceTest")
}
