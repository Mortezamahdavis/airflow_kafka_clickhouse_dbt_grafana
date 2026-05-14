"""
Runs `dbt build` (models + tests in dependency order) inside the dbt container.
Called by Airflow DAG 2 via DockerOperator.

Exit codes:
  0 — all models built and all tests passed
  1 — build or test failure (Airflow marks task as failed)
"""

import subprocess
import sys

DBT_PROJECT_DIR = "/dbt"
DBT_PROFILES_DIR = "/dbt"


def main() -> None:
    # Install packages (dbt_utils etc.) before building
    deps = subprocess.run(
        ["dbt", "deps", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR],
        text=True,
    )
    if deps.returncode != 0:
        sys.exit(deps.returncode)

    result = subprocess.run(
        [
            "dbt", "build",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROFILES_DIR,
            "--target", "prod",  # Airflow always deploys to prod
        ],
        text=True,
    )

    # Mirror dbt's exit code so Airflow sees failures correctly
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
