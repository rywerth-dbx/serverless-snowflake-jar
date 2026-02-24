#!/usr/bin/env python3
"""Deploy the serverless Snowflake JAR using the Databricks Python SDK.

Alternative to DABs — builds the JAR, uploads it to a UC Volume, and
creates/updates a scheduled job. Useful for CI/CD pipelines or teams
that prefer programmatic deployment over DABs.

Usage:
    python deploy.py --profile <your-profile> \
        --catalog <catalog> --schema <schema>

    python deploy.py --profile <your-profile> \
        --catalog <catalog> --schema <schema> --run
"""

import argparse
import subprocess
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute

JAR_LOCAL = "target/scala-2.13/serverless-snowflake-jar-assembly-0.1.0-SNAPSHOT.jar"
JAR_NAME = "serverless-snowflake-jar.jar"
MAIN_CLASS = "com.demo.ServerlessSnowflakeReader"
JOB_NAME = "serverless-snowflake-sdk-deploy"


def build_jar():
    print("Building fat JAR...")
    result = subprocess.run(["sbt", "assembly"], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(1)
    print("Build complete.")


def upload_jar(w: WorkspaceClient, volume_path: str):
    jar_path = Path(JAR_LOCAL)
    if not jar_path.exists():
        print(f"JAR not found at {JAR_LOCAL}. Run 'sbt assembly' first.")
        sys.exit(1)

    dest = f"/Volumes/{volume_path}/{JAR_NAME}"
    print(f"Uploading JAR to {dest}...")
    with open(jar_path, "rb") as f:
        w.files.upload(dest, f, overwrite=True)
    print("Upload complete.")


def find_existing_job(w: WorkspaceClient) -> int | None:
    for job in w.jobs.list(name=JOB_NAME):
        if job.settings and job.settings.name == JOB_NAME:
            return job.job_id
    return None


def create_or_update_job(w: WorkspaceClient, volume_path: str) -> int:
    jar_volume_path = f"/Volumes/{volume_path}/{JAR_NAME}"

    task = jobs.Task(
        task_key="run_snowflake_reader",
        spark_jar_task=jobs.SparkJarTask(main_class_name=MAIN_CLASS),
        environment_key="serverless_scala",
    )

    environment = jobs.JobEnvironment(
        environment_key="serverless_scala",
        spec=compute.Environment(
            client="4-scala-preview",
            java_dependencies=[jar_volume_path],
        ),
    )

    schedule = jobs.CronSchedule(
        quartz_cron_expression="0 0 9 ? * MON",
        timezone_id="America/New_York",
    )

    existing_id = find_existing_job(w)

    if existing_id:
        print(f"Updating existing job {existing_id}...")
        w.jobs.reset(
            job_id=existing_id,
            new_settings=jobs.JobSettings(
                name=JOB_NAME,
                tasks=[task],
                environments=[environment],
                schedule=schedule,
            ),
        )
        print(f"Job updated: {existing_id}")
        return existing_id
    else:
        print("Creating new job...")
        resp = w.jobs.create(
            name=JOB_NAME,
            tasks=[task],
            environments=[environment],
            schedule=schedule,
        )
        print(f"Job created: {resp.job_id}")
        return resp.job_id


def run_job(w: WorkspaceClient, job_id: int):
    print(f"Triggering job run...")
    run = w.jobs.run_now(job_id)
    print(f"Run started. Waiting for completion...")
    result = run.result()
    state = result.state
    if state and state.result_state and state.result_state.value == "SUCCESS":
        print("Run completed successfully.")
    else:
        msg = state.state_message if state else "unknown"
        print(f"Run failed: {msg}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Deploy serverless Snowflake JAR via SDK")
    parser.add_argument("--profile", required=True, help="Databricks CLI profile name")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument("--schema", required=True, help="Schema name")
    parser.add_argument("--volume", default="jars", help="Volume name (default: jars)")
    parser.add_argument("--run", action="store_true", help="Run the job after deploying")
    parser.add_argument("--skip-build", action="store_true", help="Skip sbt assembly")
    args = parser.parse_args()

    volume_path = f"{args.catalog}/{args.schema}/{args.volume}"

    w = WorkspaceClient(profile=args.profile)

    if not args.skip_build:
        build_jar()

    upload_jar(w, volume_path)
    job_id = create_or_update_job(w, volume_path)

    if args.run:
        run_job(w, job_id)

    print(f"\nDone. Job: {JOB_NAME} (ID: {job_id})")
    print(f"JAR: /Volumes/{volume_path}/{JAR_NAME}")
    print(f"Schedule: Every Monday at 9:00 AM ET")


if __name__ == "__main__":
    main()
