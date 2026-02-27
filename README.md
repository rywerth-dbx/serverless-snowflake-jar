# Serverless Snowflake JAR

Run a Spark JAR on **Databricks Serverless** that reads from a Snowflake table using the [spark-snowflake connector](https://docs.snowflake.com/en/user-guide/spark-connector) with pushdown predicates and writes results to a Unity Catalog table.

The same code runs locally via [Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/) and deployed as a serverless JAR task — no environment detection logic, no code changes between local and production.

## What it does

1. Reads Snowflake credentials from [Databricks Secrets](https://docs.databricks.com/aws/en/security/secrets/) (falls back to a `.env` file locally)
2. Connects to Snowflake using the spark-snowflake connector
3. Reads from `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS` (~1.5B rows) with a date range pushdown predicate
4. Writes the filtered results to a Unity Catalog table
5. Runs an HLL (HyperLogLog) distinct count demo comparing [Apache DataSketches](https://datasketches.apache.org/), Spark's built-in `approx_count_distinct`, and exact count

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

The deploy step uploads the fat JAR to a [Unity Catalog Volume](https://docs.databricks.com/aws/en/volumes/). Create one if you don't already have one:

```bash
databricks volumes create <catalog> <schema> jars MANAGED --profile <your-profile>
```

### 3. Update config for your environment

In `ServerlessSnowflakeReader.scala`, update `DefaultUCTable` to your catalog/schema.

For DABs deployment, also update `databricks.yml` (workspace host, Volume path in the build command) and `resources/serverless_snowflake_job.yml` (JAR path in `java_dependencies`).

## Run locally

```bash
DATABRICKS_CONFIG_PROFILE=<your-profile> DATABRICKS_SERVERLESS_COMPUTE_ID=auto sbt run
```

This uses [Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/) to execute against serverless compute from your machine. Credentials are fetched from Databricks Secrets via the REST API.

## Deploy and run on Databricks

Two deployment options are provided. Both build the JAR, upload it to a UC Volume, and create a serverless job.

### Option 1: Databricks Python SDK (`deploy.py`)

A Python script that uses the [Databricks SDK](https://docs.databricks.com/aws/en/dev-tools/sdk-python) to programmatically deploy and manage the job. This approach is useful for CI/CD pipelines or teams that prefer scripted deployments.

The SDK deploy creates a scheduled job that runs every Monday at 9:00 AM ET.

```bash
# Install the SDK (if not already installed)
pip install databricks-sdk

# Build, upload, and create/update the job
python deploy.py --profile <your-profile> --catalog <catalog> --schema <schema>

# Build, upload, create/update, and run immediately
python deploy.py --profile <your-profile> --catalog <catalog> --schema <schema> --run

# Skip the build step (if JAR is already built)
python deploy.py --profile <your-profile> --catalog <catalog> --schema <schema> --skip-build --run
```

The script is idempotent — it creates the job on first run and updates it on subsequent runs.

### Option 2: Databricks Asset Bundles (DABs)

[DABs](https://docs.databricks.com/aws/en/dev-tools/bundles/) is a CLI-driven tool for managing Databricks resources as code. It defines jobs, clusters, and other resources in YAML files that live alongside your source code. Running `bundle deploy` builds the JAR, uploads it to the UC Volume, and creates/updates the job definition in a single step.

```bash
# Build JAR, upload to UC Volume, and deploy the job
databricks bundle deploy -t dev --profile <your-profile>

# Run the job
databricks bundle run -t dev serverless_snowflake_test --profile <your-profile>
```

## Key design decisions

- **`DatabricksSession.builder().getOrCreate()`** — single entry point, works in both environments. Locally it creates a Spark Connect client; on serverless it picks up the runtime session.
- **`DBUtils.getDBUtils().secrets.get()`** — reads credentials from Databricks Secrets. Works locally (REST API via your profile) and on serverless (native dbutils). No credentials in job config or source code.
- **`sfDriver` option** — explicitly registers the Snowflake JDBC driver. Required on serverless because the runtime's classloader doesn't auto-discover JDBC drivers from uploaded JARs.
- **Fat JAR excludes Spark/Databricks classes** — these are provided by the serverless runtime. The Snowflake connector and JDBC driver are included.
- **Apache DataSketches for HLL** — vendor-neutral HyperLogLog library that works on any Spark environment (EKS, Databricks, standalone). Included as a cross-environment alternative to Databricks-specific `hll_sketch_agg()` or the unmaintained spark-alchemy library.

## Gotchas

- `{{secrets/scope/key}}` in task parameters **does not work** — that syntax is only for Spark conf and cluster env vars, neither of which are available on serverless. Use `dbutils.secrets.get()` in code instead.
- The `assemblyExcludedJars` filter excludes `spark-*` JARs but must **not** exclude `spark-snowflake` — the connector needs to be in the fat JAR.
- Serverless JAR tasks are **Public Preview** and must be enabled on your workspace.
- **Custom UDAFs and HLL functions are blocked on serverless** — Unity Catalog shared access mode does not allow user-defined aggregate functions or Databricks built-in `hll_sketch_agg()`. Libraries like DataSketches or spark-alchemy that rely on custom Spark `Aggregator` classes won't work, and even `hll_sketch_agg` is treated as a user-defined aggregator by UC security. Only standard Spark aggregates like `approx_count_distinct()` work on serverless. The demo's `hllDistinctCount()` uses a 3-tier fallback chain that picks the best available engine for the current environment.

## Reference docs

- [Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/)
- [JARs on Serverless](https://docs.databricks.com/aws/en/jobs/how-to/use-jars-in-workflows)
- [JAR Task for Jobs](https://docs.databricks.com/aws/en/jobs/jar)
- [Spark Properties for Serverless](https://docs.databricks.com/aws/en/spark/conf#configure-spark-properties-for-serverless-notebooks-and-jobs)
- [Databricks Secrets](https://docs.databricks.com/aws/en/security/secrets/)
- [Unity Catalog Volumes](https://docs.databricks.com/aws/en/volumes/)
- [Databricks Asset Bundles for Scala JARs](https://docs.databricks.com/aws/en/dev-tools/bundles/scala-jar)
- [Serverless Limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
