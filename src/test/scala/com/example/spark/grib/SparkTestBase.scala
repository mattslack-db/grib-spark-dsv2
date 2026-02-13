package com.example.spark.grib

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

/**
 * Shared test infrastructure for GRIB data source tests.
 * Provides a local SparkSession and common test helpers.
 */
trait SparkTestBase extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("GribDataSourceTest")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  /** Load a test resource file path from the classpath. */
  def testResource(name: String): String = {
    val url = getClass.getClassLoader.getResource(name)
    require(url != null, s"Test resource not found: $name")
    url.getPath
  }

  /** Configure Spark for anonymous S3 access (NOAA public buckets). */
  def configureAnonymousS3(): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    )
    hadoopConf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoopConf.set("fs.s3a.region", "us-east-1")
  }
}
