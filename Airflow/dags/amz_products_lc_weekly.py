# amz_product_lc_weekly.py
from __future__ import annotations
from datetime import timedelta
import pendulum
from docker.types import Mount

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# ────────────────────────────────  CONSTANTS  ──────────────────────────────── #

SLACK_CONN_ID = "slack_webhook_conn"

SCRAPER_LOGS_PATH = (
    "D:/Documents/GitHub/e-commerce.worktrees/airflow_init/scraper_logs"
)

CATEGORY_SCHEDULE = {
    0: ["sports"],
    1: ["kitchen", "hpc"],
    2: ["industrial", "automotive"],
    3: ["grocery", "garden", "pet-supplies"],
    4: ["home-improvement", "beauty", "baby", "shoes", "office", "jewelry", "luggage"],
    5: ["electronics"],
    6: [],  # catch-up day – scrape unfinished lowest-level cats
}

DOCKER_DEFAULTS = {
    "image": "scraper-project:latest",
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": "host",
    "auto_remove": "success",
    "mount_tmp_dir": False,
    "mounts": [
        Mount(source=SCRAPER_LOGS_PATH, target="/app/logs", type="bind"),
    ],
    "tty": True,
}

# ───────────────────────  GENERIC SLACK CALLBACKS  ────────────────────────── #


def slack_callback(color: str, prefix: str):
    """
    Returns a tiny wrapper callable that Airflow will invoke on failure / retry.
    """
    def _post_to_slack(context):
        SlackWebhookOperator(
            task_id=f"slack_{prefix}",
            slack_webhook_conn_id=SLACK_CONN_ID,
            message=f":{color}_circle: *{prefix.title()}* in DAG "
            f"`{context['dag'].dag_id}` – run `{context['run_id']}`",
        ).execute(context=context)

    return _post_to_slack


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=13),
    "on_failure_callback": slack_callback("red", "failure"),
    "on_retry_callback": slack_callback("orange", "retry"),
}

# ────────────────────────────────  DAG  ───────────────────────────────────── #

with DAG(
    dag_id="amz_product_lc_weekly",
    description="Week-day-wise lowest-category Amazon product crawler",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Kolkata"),
    schedule="0 11 * * *",  # 11 AM IST every day
    catchup=False,
    default_args=default_args,
    tags=["amazon", "scrapy"],
) as dag:

    # 1 — DAG-level start ping
    dag_start = SlackWebhookOperator(
        task_id="slack_dag_start",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=""":rocket: *Run started*
            *DAG*: `{{ dag.dag_id }}`
            *Run ID*: `{{ run_id }}`""",
    )

    # 2 — Pick categories for TODAY (done at *parse* time, so tasks are visible)
    today_index = pendulum.today("Asia/Kolkata").weekday()
    categories_today = CATEGORY_SCHEDULE.get(today_index, [])

    # If you prefer “always have at least one task” you can handle len == 0 here
    if not categories_today:
        nothing_to_do = SlackWebhookOperator(
            task_id="no_categories",
            slack_webhook_conn_id=SLACK_CONN_ID,
            message=":grey_question: No categories scheduled today – nothing to crawl.",
        )
        dag_start >> nothing_to_do
    else:
        previous = dag_start
        for idx, cat in enumerate(categories_today):
            # 2a — Slack “begin category”
            start_cat = SlackWebhookOperator(
                task_id=f"start_{cat}",
                slack_webhook_conn_id=SLACK_CONN_ID,
                message=f":fast_forward: Starting *{cat}*",
            )

            # 2b — Docker-run Scrapy spider
            scrape_cat = DockerOperator(
                task_id=f"scrape_{cat}",
                api_version="auto",
                command=(
                    "scrapy crawl AmzProductsLC "
                    f"-a category={cat} "
                    "-a batch_size=100 "
                    "-s DEPTH_LIMIT=10 "
                    f"-a logfile=/app/logs/amzProductsLC_{cat}_{{{{ ds }}}}.log"
                ),
                **DOCKER_DEFAULTS,
            )

            # 2c — Slack “done category”
            next_cat = categories_today[idx + 1] if idx + 1 < len(categories_today) else None
            end_msg = f":white_check_mark: Finished *{cat}*." + (
                f" Next: *{next_cat}*" if next_cat else ""
            )
            end_cat = SlackWebhookOperator(
                task_id=f"end_{cat}",
                slack_webhook_conn_id=SLACK_CONN_ID,
                message=end_msg,
            )

            previous >> start_cat >> scrape_cat >> end_cat
            previous = end_cat  # chain them serially

        # 3 — DAG-level success ping
        dag_success = SlackWebhookOperator(
            task_id="slack_dag_success",
            slack_webhook_conn_id=SLACK_CONN_ID,
            message=":tada: *Run succeeded* – all categories done.",
        )
        previous >> dag_success
