scalaVersion := "2.13.16"

libraryDependencies += "com.databricks" %% "databricks-connect" % "17.0.+"

// Snowflake connector - bundled in fat JAR.
// NOTE: Verify the latest 2.13 / Spark 4.0 compatible version at:
// https://docs.snowflake.com/en/user-guide/spark-connector-install
libraryDependencies += "net.snowflake" % "spark-snowflake_2.13" % "3.1.5"
libraryDependencies += "net.snowflake" % "snowflake-jdbc" % "4.0.1"

javacOptions ++= Seq("-source", "17", "-target", "17")
scalacOptions ++= Seq("-release", "17")

fork := true
javaOptions += "--add-opens=java.base/java.nio=ALL-UNNAMED"

// Fat JAR settings
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case "reference.conf"                     => MergeStrategy.concat
  case _                                    => MergeStrategy.first
}

// Exclude Databricks Connect + Spark from fat JAR (provided by serverless runtime)
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp.filter { f =>
    val name = f.data.getName
    name.startsWith("databricks-connect") ||
    name.startsWith("databricks-sdk") ||
    (name.startsWith("spark-") && !name.startsWith("spark-snowflake")) ||
    name.startsWith("scala-library") ||
    name.contains("grpc") ||
    name.contains("protobuf") ||
    name.contains("netty")
  }
}
