# File: airflow/dags/amz_categories_daily.py
from __future__ import annotations
from docker.types import Mount

import pendulum

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup

# For your scraper logs
SCRAPER_LOGS_PATH = "D:/Documents/GitHub/e-commerce.worktrees/airflow_init/scraper_logs"

# For your main scrapers project directory
SCRAPERS_SHARED_DATA_PATH = "D:/Documents/GitHub/e-commerce.worktrees/airflow_init/shared_data"

DOCKER_DEFAULTS = {
    "image": "scraper-project:latest",
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": "host",
    "auto_remove": "success",
    'mount_tmp_dir': False,
    "mounts": [
        # This is the corrected format
        Mount(source=SCRAPER_LOGS_PATH, target="/app/logs", type="bind"),
        Mount(source=SCRAPERS_SHARED_DATA_PATH, target="/app/.urls_to_scrap", type="bind"),
        # You may also need to mount the scraper source itself if the image
        # doesn't contain the latest version of the code, though it should.
        # For now, let's just fix the log mount which is likely the cause.
    ],
    "tty": True,
}

with DAG(
    dag_id="amz_categories_daily",
    start_date=pendulum.datetime(2025, 6, 15, tz="Asia/Kolkata"),
    schedule="0 22 * * *", # 10 PM IST
    catchup=False,
    tags=["amazon", "scrapy"],
) as dag:
    list_types = ["bestsellers", "movers_and_shakers", "hot_new_releases", "most_wished_for"]

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