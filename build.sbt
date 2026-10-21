name := "spark-grib"
version := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"
val netcdfVersion = "5.6.0"

libraryDependencies ++= Seq(
  // Spark (provided — user's cluster supplies these)
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",

  // GRIB parsing
  "edu.ucar" % "grib"     % netcdfVersion,
  "edu.ucar" % "cdm-core" % netcdfVersion,

  // S3 support (provided — typically on cluster). hadoop-aws must match the
  // Hadoop version Spark ships (3.3.4 for Spark 3.5.x), or S3AFileSystem fails
  // to load against Spark's hadoop-client-api; 1.12.262 is the SDK it was built with.
  "org.apache.hadoop" % "hadoop-aws"            % "3.3.4" % "provided",
  "com.amazonaws"     % "aws-java-sdk-bundle"   % "1.12.262" % "provided",

  // Testing
  "org.scalatest"    %% "scalatest"  % "3.2.18" % "test",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test"
)

// Unidata repository for netCDF-Java
resolvers += "Unidata" at "https://artifacts.unidata.ucar.edu/repository/unidata-all/"

// Build fat JAR excluding Spark/Hadoop
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case "module-info.class"                  => MergeStrategy.discard
  case x if x.endsWith(".proto")            => MergeStrategy.first
  case x if x.endsWith(".class")            => MergeStrategy.first
  case _                                    => MergeStrategy.first
}

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.**" -> "shaded.com.google.@1").inAll
)

// Test configuration
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  // Pin the driver/block-manager host to loopback so local Spark tests don't
  // fail with block-manager connect errors when the machine has a LAN IP.
  "-Dspark.driver.host=127.0.0.1"
)

// Exclude slow tests by default
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "com.example.spark.grib.tags.SlowTest")

// `sbt testAll` also runs the SlowTest-tagged integration tests (fetch their
// fixtures with ./download_test_fixtures.sh first). Clearing the exclusion is
// needed because ScalaTest's -l always wins over an -n include.
addCommandAlias("testAll", "; set Test / testOptions := Seq() ; test")
