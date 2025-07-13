# File: airflow/dags/amz_categories_daily.py
from __future__ import annotations
from docker.types import Mount

import pendulum

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from common.notifications import slack_notification
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_CONN_ID = "slack_webhook_conn"
SCRAPER_LOGS_PATH = "D:/Documents/GitHub/e-commerce.worktrees/airflow_init/scraper_logs"
SCRAPERS_SHARED_DATA_PATH = "D:/Documents/GitHub/e-commerce.worktrees/airflow_init/shared_data"
DOCKER_DEFAULTS = {
    "image": "scraper-project:latest",
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": "host",
    "auto_remove": "success",
    'mount_tmp_dir': False,
    "mounts": [
        Mount(source=SCRAPER_LOGS_PATH, target="/app/logs", type="bind"),
        Mount(source=SCRAPERS_SHARED_DATA_PATH, target="/app/.urls_to_scrap", type="bind"),
    ],
    "tty": True,
}

with DAG(
    dag_id="amz_categories_daily",
    start_date=pendulum.datetime(2025, 6, 15, tz="Asia/Kolkata"),
    schedule="0 22 * * *", # 10 PM IST
    catchup=False,
    on_success_callback=slack_notification,
    on_failure_callback=slack_notification,
    tags=["amazon", "scrapy"],
) as dag:
    list_types = ["bestsellers", "movers_and_shakers", "hot_new_releases", "most_wished_for"]
    # Send a notification when the DAG starts
    start_notification = SlackWebhookOperator(
        task_id=f"{dag.dag_id}_start_notification",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=f""":large_yellow_circle: *DAG Started*
            *DAG*: `{dag.dag_id}`
            *Run ID*: `{{{{ run_id }}}}`""",
    )

    task_groups = []
    for list_type in list_types:
        with TaskGroup(group_id=f"group_{list_type}") as tg:
            # Task 1: Get Category URLs
            get_urls = DockerOperator(
                task_id=f"get_urls_{list_type}",
                command=f"scrapy crawl AmzCategoryUrls -a list_type={list_type} -a logfile=./logs/amz_category_urls_{list_type}.log",
                **DOCKER_DEFAULTS,
            )

            # Task 2: Scrape Categories from the URLs
            scrape_category = DockerOperator(
                task_id=f"scrape_category_{list_type}",
                command=f"scrapy crawl AmzCategory -a list_type={list_type} -a batch_size=500 -a logfile=./logs/amz_category_{list_type}.log",
                **DOCKER_DEFAULTS,
            )

            get_urls >> scrape_category
        task_groups.append(tg)
    start_notification >> task_groups