package com.demo

import com.databricks.connect.DatabricksSession
import com.databricks.sdk.scala.dbutils.DBUtils
import org.apache.spark.sql.{DataFrame, SparkSession, Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
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
   * Used on standard Spark / EKS where custom UDAFs are allowed.
   */
  object HllEstimator extends Aggregator[String, Array[Byte], Long] {
    private val LogK = 12

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

  /**
   * Cross-environment HLL distinct count.
   * - On Databricks: uses built-in hll_sketch_agg (no UDAF, works on serverless)
   * - On standard Spark / EKS: uses Apache DataSketches UDAF
   *
   * Same function, same result, different engine under the hood.
   */
  /**
   * Cross-environment HLL distinct count with automatic fallback.
   *
   * Tries in order:
   * 1. Databricks built-in hll_sketch_agg (works on single-user mode)
   * 2. Apache DataSketches UDAF (works on EKS / standard Spark)
   * 3. Spark built-in approx_count_distinct (works everywhere)
   *
   * Returns (estimate, engine_used).
   */
  def hllDistinctCount(df: DataFrame, column: String, spark: SparkSession): (Long, String) = {
    // Try Databricks built-in HLL
    val attempt1 = Try {
      df.createOrReplaceTempView("hll_input")
      val result = spark.sql(s"SELECT hll_sketch_estimate(hll_sketch_agg($column)) FROM hll_input")
        .collect()(0).getLong(0)
      (result, "Databricks hll_sketch_agg")
    }
    if (attempt1.isSuccess) return attempt1.get

    // Try DataSketches UDAF (works on standard Spark / EKS)
    val attempt2 = Try {
      val hll = udaf(HllEstimator)
      val result = df.select(col(column).cast("string"))
        .agg(hll(col(column)).as("hll_estimate"))
        .collect()(0).getLong(0)
      (result, "Apache DataSketches UDAF")
    }
    if (attempt2.isSuccess) return attempt2.get

    // Fallback: built-in approx_count_distinct (works everywhere)
    val result = df.agg(approx_count_distinct(column).as("approx"))
      .collect()(0).getLong(0)
    (result, "Spark approx_count_distinct (fallback)")
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
      // Demonstrates cross-environment HLL using hllDistinctCount().
      // On Databricks: uses built-in hll_sketch_agg (no UDAF)
      // On EKS / standard Spark: uses Apache DataSketches UDAF
      println()
      println("=" * 60)
      println("HLL (HyperLogLog) Distinct Count Demo")
      println("=" * 60)

      // Cross-environment HLL — tries Databricks built-in first, falls back to DataSketches
      val (hllResult, hllEngine) = hllDistinctCount(ordersDF, "O_CUSTKEY", spark)
      println(s"HLL engine used:                      $hllEngine")
      println(s"HLL estimate (distinct customers):    $hllResult")

      // Built-in approx_count_distinct for comparison (works everywhere, no library needed)
      val approxResult = ordersDF
        .agg(approx_count_distinct("O_CUSTKEY").as("approx_distinct"))
        .collect()(0).getLong(0)
      println(s"approx_count_distinct estimate:       $approxResult")

      // Exact count for comparison
      val exactResult = ordersDF.select("O_CUSTKEY").distinct().count()
      println(s"Exact distinct count:                 $exactResult")
      println()

      println("=" * 60)
      println("FEASIBILITY TEST PASSED")
      println("=" * 60)
    } finally {
      spark.stop()
    }
  }
}
