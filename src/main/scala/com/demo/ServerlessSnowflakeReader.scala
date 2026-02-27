package com.demo

import com.databricks.connect.DatabricksSession
import com.databricks.sdk.scala.dbutils.DBUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.functions.{approx_count_distinct, col, udaf}
import org.apache.datasketches.hll.{HllSketch, Union}
import scala.io.Source
import scala.util.Try

/**
 * Feasibility test: run a Spark JAR on Databricks Serverless that reads
 * from a Snowflake table with a pushdown predicate.
 *
 * Mirrors the customer's EKS pattern:
 *   Spark JAR -> Snowflake connector -> read with predicate -> return results
 *
 * Works identically locally (sbt run) and deployed on Serverless (JAR task).
 * Credentials are read from Databricks Secrets (works in both environments),
 * with a .env file fallback for local development.
 */
object ServerlessSnowflakeReader {

  /**
   * HLL (HyperLogLog) aggregator using Apache DataSketches.
   * Estimates distinct counts using very little memory.
   * Works on any Spark environment (EKS, Databricks, local) — no vendor lock-in.
   */
  object HllEstimator extends Aggregator[String, Array[Byte], Long] {
    private val LogK = 12 // precision parameter (higher = more accurate, more memory)

    def zero: Array[Byte] = new HllSketch(LogK).toCompactByteArray

    def reduce(buf: Array[Byte], value: String): Array[Byte] = {
      val sketch = HllSketch.heapify(buf)
      sketch.update(value)
      sketch.toCompactByteArray
    }

    def merge(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
      val union = new Union(LogK)
      union.update(HllSketch.heapify(a))
      union.update(HllSketch.heapify(b))
      union.getResult.toCompactByteArray
    }

    def finish(buf: Array[Byte]): Long =
      HllSketch.heapify(buf).getEstimate.toLong

    def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY
    def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  val SnowflakeFormat = "net.snowflake.spark.snowflake"
  val SecretScope = "snowflake"
  val DefaultUCTable = "ryan_werth_workspace_catalog.serverless_snowflake_demo.snowflake_orders"

  private def loadDotEnv(): Map[String, String] = {
    val file = new java.io.File(".env")
    if (!file.exists()) return Map.empty
    val source = Source.fromFile(file)
    try {
      source.getLines()
        .map(_.trim)
        .filter(line => line.nonEmpty && !line.startsWith("#"))
        .flatMap { line =>
          line.split("=", 2) match {
            case Array(k, v) => Some(k.trim -> v.trim.stripPrefix("\"").stripSuffix("\""))
            case _ => None
          }
        }
        .toMap
    } finally {
      source.close()
    }
  }

  private def getCredentials(): (String, String, String, String) = {
    // Try Databricks Secrets first (works locally via REST API and on serverless via native dbutils)
    val fromSecrets = Try {
      val dbutils = DBUtils.getDBUtils()
      val url = dbutils.secrets.get(scope = SecretScope, key = "url")
      val user = dbutils.secrets.get(scope = SecretScope, key = "user")
      val password = dbutils.secrets.get(scope = SecretScope, key = "password")
      (url, user, password, "COMPUTE_WH")
    }

    fromSecrets.getOrElse {
      // Fall back to .env file
      val env = loadDotEnv()
      (env.getOrElse("SNOWFLAKE_URL", ""),
       env.getOrElse("SNOWFLAKE_USER", ""),
       env.getOrElse("SNOWFLAKE_PASSWORD", ""),
       env.getOrElse("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    }
  }

  def main(args: Array[String]): Unit = {
    val (sfURL, sfUser, sfPassword, sfWarehouse) = getCredentials()
    val ucTable = if (args.nonEmpty) args(0) else DefaultUCTable

    require(sfURL.nonEmpty && sfUser.nonEmpty && sfPassword.nonEmpty,
      "No credentials found. Set up Databricks Secrets (scope: snowflake, keys: url/user/password) or create a .env file (see .env.example)")

    val spark: SparkSession = DatabricksSession.builder().getOrCreate()

    try {
      val sfOptions = Map(
        "sfURL"       -> sfURL,
        "sfUser"      -> sfUser,
        "sfPassword"  -> sfPassword,
        "sfDatabase"  -> "SNOWFLAKE_SAMPLE_DATA",
        "sfSchema"    -> "TPCH_SF1000",
        "sfWarehouse" -> sfWarehouse,
        "sfDriver"    -> "net.snowflake.client.jdbc.SnowflakeDriver"
      )

      println("=" * 60)
      println("Serverless Snowflake Feasibility Test")
      println("=" * 60)
      println(s"Snowflake URL:   $sfURL")
      println(s"Source table:    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS")
      println(s"Connector:       $SnowflakeFormat")
      println(s"Target UC table: $ucTable")
      println()

      val ordersDF = spark.read
        .format(SnowflakeFormat)
        .options(sfOptions)
        .option("dbtable", "ORDERS")
        .load()
        .filter("O_ORDERDATE >= '1997-01-01' AND O_ORDERDATE < '1997-04-01'")

      val count = ordersDF.count()
      println(s"Records matching predicate: $count")
      ordersDF.show(20)

      ordersDF.write
        .mode("overwrite")
        .saveAsTable(ucTable)

      println()
      println(s"Results written to $ucTable")

      // --- HLL (HyperLogLog) Demo ---
      // Demonstrates Apache DataSketches for approximate distinct counts.
      // This is a cross-environment approach: works on EKS Spark AND Databricks.
      println()
      println("=" * 60)
      println("HLL (HyperLogLog) Distinct Count Demo")
      println("=" * 60)

      // Approach 1: Apache DataSketches (cross-environment, gives raw sketches)
      // Note: Custom UDAFs may be restricted via Databricks Connect (shared access mode).
      // Works on EKS Spark and may work on serverless JAR tasks.
      Try {
        val hll = udaf(HllEstimator)
        val hllResult = ordersDF
          .select(col("O_CUSTKEY").cast("string"))
          .agg(hll(col("O_CUSTKEY")).as("hll_estimate"))
          .collect()(0).getLong(0)
        println(s"DataSketches HLL estimate (distinct customers): $hllResult")
      }.recover { case e: Exception =>
        println(s"DataSketches UDAF not available in this environment: ${e.getMessage}")
        println("  (Custom UDAFs are restricted in UC shared access mode.)")
        println("  (This works on standard Spark / EKS.)")
      }

      // Approach 2: Built-in approx_count_distinct (simpler, no library needed, works everywhere)
      val approxResult = ordersDF
        .agg(approx_count_distinct("O_CUSTKEY").as("approx_distinct"))
        .collect()(0).getLong(0)
      println(s"Spark approx_count_distinct estimate:           $approxResult")

      // Exact count for comparison
      val exactResult = ordersDF.select("O_CUSTKEY").distinct().count()
      println(s"Exact distinct count:                           $exactResult")
      println()

      println("=" * 60)
      println("FEASIBILITY TEST PASSED")
      println("=" * 60)
    } finally {
      spark.stop()
    }
  }
}
