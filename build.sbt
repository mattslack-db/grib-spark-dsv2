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

  // S3 support (provided — typically on cluster)
  "org.apache.hadoop" % "hadoop-aws"            % "3.3.6" % "provided",
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
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
)

// Exclude slow tests by default
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "com.example.spark.grib.tags.SlowTest")
