# Serverless Snowflake JAR Feasibility Test

## Status: PASSED (Local + Serverless)

Tested 2026-02-23. Read 56M records from Snowflake TPCH_SF1000.ORDERS with pushdown predicate,
wrote to Unity Catalog table, all on Databricks Serverless compute.

## What This Is

A feasibility demo for a customer migrating Spark workloads from AWS EKS to Databricks Serverless.
The customer has a "cleanroom" architecture where a Spark JAR reads from Snowflake via the
spark-snowflake connector with pushdown predicates, running on EKS today.

## Customer Context

- **Slack thread**: https://databricks.slack.com/archives/C0ADC1GG3NX/p1771879724532929
- **Current setup**: Spark JAR on EKS -> spark-snowflake connector -> Snowflake native table
- **Source table**: `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS` (~1.5B rows)
- **Connector**: `spark-snowflake_2.12-3.1.1.jar` (needs upgrade to 2.13 for serverless)
- **Key requirement**: Pushdown predicates must work

## Architecture

```
Snowflake Trial (vggymaj-pnb87710)    Databricks (fevm-ryan-werth-workspace)
┌──────────────────────┐               ┌────────────────────────────┐
│ TPCH_SF1000.ORDERS   │◄─────────────►│ Serverless Compute         │
│ (~1.5B rows)         │               │  - Scala 2.13 JAR          │
└──────────────────────┘               │  - spark-snowflake 3.1.5   │
                                       │  - Pushdown predicate      │
                                       │  Results → UC table        │
                                       └────────────────────────────┘
```

## Tech Stack

- **Language**: Scala 2.13.16
- **Build**: sbt 1.11.7 + sbt-assembly (fat JAR)
- **Databricks Connect**: `com.databricks:databricks-connect:17.0.+`
- **Snowflake**: `spark-snowflake_2.13:3.1.5` + `snowflake-jdbc:4.0.1`
- **Deployment**: Databricks Asset Bundles
- **Compute**: Databricks Serverless (JAR task, Public Preview)

## How to Run

### Local (via Databricks Connect)

Credentials are read from Databricks Secrets (scope: `snowflake`) via REST API.
Falls back to `.env` file if secrets are unavailable.

```bash
cd ~/Documents/Databricks/serverless-snowflake-jar
DATABRICKS_CONFIG_PROFILE=fevm-ryan-werth DATABRICKS_SERVERLESS_COMPUTE_ID=auto sbt run
```

### Deploy as Serverless JAR Task

```bash
sbt clean assembly
databricks fs cp target/scala-2.13/serverless-snowflake-jar-assembly-0.1.0-SNAPSHOT.jar \
  dbfs:/Volumes/ryan_werth_workspace_catalog/default/jars/serverless-snowflake-jar-assembly-0.1.0-SNAPSHOT.jar \
  --overwrite --profile fevm-ryan-werth
databricks bundle deploy -t dev --profile fevm-ryan-werth
databricks bundle run -t dev serverless_snowflake_test --profile fevm-ryan-werth
```

## Key Files

- `src/main/scala/com/demo/ServerlessSnowflakeReader.scala` - single entry point, works locally and deployed
- `build.sbt` - follows Databricks Connect Scala JAR docs pattern
- `databricks.yml` + `resources/serverless_snowflake_job.yml` - DAB deployment config
- `.env` / `.env.example` - local credential fallback

## Credentials

Credentials are managed via **Databricks Secrets** (scope: `snowflake`, keys: `url`, `user`, `password`).
The code uses `DBUtils.getDBUtils().secrets.get()` which works in both environments:
- **Locally**: uses the Databricks REST API via your configured profile
- **On serverless**: proxies to the runtime's native dbutils

Falls back to `.env` file for local development if secrets are unavailable.

## Snowflake Account

- **Account**: `vggymaj-pnb87710`
- **URL**: https://app.snowflake.com/vggymaj/pnb87710
- **Expires**: ~2026-03-25

## Gotchas / Lessons Learned

1. **`{{secrets/scope/key}}` does NOT work in task parameters** — it's only for Spark conf and cluster env vars, neither available on serverless.
2. **JDBC driver classloader issue on serverless** — set `sfDriver` option to `net.snowflake.client.jdbc.SnowflakeDriver` so the connector explicitly loads the driver via `Class.forName`.
3. **Serverless JAR tasks are Public Preview** — must be enabled on the workspace.
4. **Fat JAR exclusions** — exclude `spark-*` JARs from assembly but NOT `spark-snowflake`.
5. **`DatabricksSession.builder().getOrCreate()`** — same call works locally and on serverless. No detection logic needed.

## Relevant Docs

- [Databricks Connect Scala examples](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/scala/examples)
- [Databricks Connect Scala JAR compilation](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/scala/jar-compile)
- [JARs on Serverless](https://docs.databricks.com/aws/en/jobs/how-to/use-jars-in-workflows)
- [JAR task for Jobs](https://docs.databricks.com/aws/en/jobs/jar)
- [DABs for Scala JARs](https://docs.databricks.com/aws/en/dev-tools/bundles/scala-jar)
- [Serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
- [Dynamic value references](https://docs.databricks.com/aws/en/jobs/dynamic-value-references)
