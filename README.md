# Serverless Snowflake JAR

Run a Spark JAR on **Databricks Serverless** that reads from a Snowflake table using the [spark-snowflake connector](https://docs.snowflake.com/en/user-guide/spark-connector) with pushdown predicates and writes results to a Unity Catalog table.

The same code runs locally via Databricks Connect and deployed as a serverless JAR task — no environment detection logic, no code changes between local and production.

## What it does

1. Reads Snowflake credentials from [Databricks Secrets](https://docs.databricks.com/aws/en/security/secrets/) (falls back to a `.env` file locally)
2. Connects to Snowflake using the spark-snowflake connector
3. Reads from `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS` (~1.5B rows) with a date range pushdown predicate
4. Writes the filtered results to a Unity Catalog table

## Prerequisites

- JDK 17
- [sbt](https://www.scala-sbt.org/)
- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) with a configured auth profile
- A Databricks workspace with **serverless JAR tasks enabled** (Public Preview)
- A Snowflake account with access to `SNOWFLAKE_SAMPLE_DATA`

## Setup

### 1. Store Snowflake credentials in Databricks Secrets

```bash
databricks secrets create-scope snowflake --profile <your-profile>
databricks secrets put-secret snowflake url --string-value "<account>.snowflakecomputing.com" --profile <your-profile>
databricks secrets put-secret snowflake user --string-value "<username>" --profile <your-profile>
databricks secrets put-secret snowflake password --string-value "<password>" --profile <your-profile>
```

### 2. Create a UC Volume for the JAR

```bash
databricks volumes create <catalog>.<schema>.jars --volume-type MANAGED --profile <your-profile>
```

### 3. Update config for your environment

In `databricks.yml`, set your workspace host:

```yaml
targets:
  dev:
    workspace:
      host: https://<your-workspace>.cloud.databricks.com
```

In `resources/serverless_snowflake_job.yml`, update the JAR path and (optionally) the target UC table:

```yaml
java_dependencies:
  - /Volumes/<catalog>/<schema>/jars/serverless-snowflake-jar-assembly-0.1.0-SNAPSHOT.jar
```

In `ServerlessSnowflakeReader.scala`, update `DefaultUCTable` to your catalog/schema.

## Run locally

```bash
DATABRICKS_CONFIG_PROFILE=<your-profile> DATABRICKS_SERVERLESS_COMPUTE_ID=auto sbt run
```

This uses Databricks Connect to execute against serverless compute from your machine. Credentials are fetched from Databricks Secrets via the REST API.

## Deploy and run on Databricks

```bash
# Build fat JAR
sbt clean assembly

# Upload to UC Volume
databricks fs cp target/scala-2.13/serverless-snowflake-jar-assembly-0.1.0-SNAPSHOT.jar \
  dbfs:/Volumes/<catalog>/<schema>/jars/serverless-snowflake-jar-assembly-0.1.0-SNAPSHOT.jar \
  --overwrite --profile <your-profile>

# Deploy and run
databricks bundle deploy -t dev --profile <your-profile>
databricks bundle run -t dev serverless_snowflake_test --profile <your-profile>
```

## Key design decisions

- **`DatabricksSession.builder().getOrCreate()`** — single entry point, works in both environments. Locally it creates a Spark Connect client; on serverless it picks up the runtime session.
- **`DBUtils.getDBUtils().secrets.get()`** — reads credentials from Databricks Secrets. Works locally (REST API via your profile) and on serverless (native dbutils). No credentials in job config or source code.
- **`sfDriver` option** — explicitly registers the Snowflake JDBC driver. Required on serverless because the runtime's classloader doesn't auto-discover JDBC drivers from uploaded JARs.
- **Fat JAR excludes Spark/Databricks classes** — these are provided by the serverless runtime. The Snowflake connector and JDBC driver are included.

## Gotchas

- `{{secrets/scope/key}}` in task parameters **does not work** — that syntax is only for Spark conf and cluster env vars, neither of which are available on serverless. Use `dbutils.secrets.get()` in code instead.
- The `assemblyExcludedJars` filter excludes `spark-*` JARs but must **not** exclude `spark-snowflake` — the connector needs to be in the fat JAR.
- Serverless JAR tasks are **Public Preview** and must be enabled on your workspace.
