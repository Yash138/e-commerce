import requests
from airflow.hooks.base import BaseHook


def slack_notification(context):
    """
    Sends a Slack notification for a DAG run's final status (success or failure).
    The webhook URL is retrieved from a parameterized Airflow Connection.
    """
    SLACK_CONN_ID = "slack_webhook_conn"
    dag_run = context.get("dag_run")
    state = dag_run.state
    dag_id = dag_run.dag_id
    log_url = context.get("task_instance").log_url

    if state == "success":
        emoji = ":large_green_circle:"
        title = f"DAG Successful: {dag_id}"
    else:  # state == "failed"
        emoji = ":red_circle:"
        title = f"DAG Failed: {dag_id}"

    # Construct the message payload
    message = f"""
    {emoji} *{title}*
    *DAG*: `{dag_id}`
    *Status*: {state}
    *Execution Date*: {dag_run.execution_date}
    *Log URL*: <{log_url}|View Log>
    """

    try:
        # Retrieve the full webhook URL from the 'host' field of the Airflow Connection
        webhook_url = BaseHook.get_connection(SLACK_CONN_ID).host
        print(f"Sending Slack notification to {webhook_url}")
        requests.post(webhook_url, json={"text": message})
    except Exception as e:
        print(f"Error sending Slack notification via Python callable: {e}")