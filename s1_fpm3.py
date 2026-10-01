from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow import DAG
from airflow.models import Variable
import json
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import DagRun
from airflow.utils.trigger_rule import TriggerRule
import requests

name="S1_FPM3"

SETUP_SCRIPT = "/home/ubuntu/insarscripts/stack_processor_aws/env_setup/setup_fpm3_aws.sh"

# No FPM3 Slack webhook connection exists yet; FPM3 posts to the FPM2 channel
# until one is added.
SLACK_CONN_ID = 'slack_webhook_fpm2'
SLACK_CHANNEL = '#fpm2-sarfinder-aws-hpc'

def ssh_cmd(script: str, use_dir: bool = True) -> str:
    # '&&' so that a missing FPM3 environment fails the task instead of
    # running the step without it. Commands must not end in ".sh": SSHOperator
    # would treat them as template files, hence the trailing "" arguments.
    base = "source " + SETUP_SCRIPT
    if use_dir:
        base += " && cd urgent_response/{{ var.json[run_id].dir_name }}"
    return base + " && " + script


def auto_control(run: str, gpu: bool = False) -> str:
    cmd = f'04_auto_control.sh "{{{{ var.json[run_id].dir_name }}}}_{run}" "start" "{run}" "{run}"'
    if gpu:
        # Queue, cores and GPU request come from the run's config.txt.
        cmd = ('source config.txt && AUTO_QUEUE=$fpm3_gpu_queue AUTO_NCORE=$fpm3_gpu_ncore '
               'AUTO_GRES=$fpm3_gpu_gres ' + cmd)
    return ssh_cmd(cmd)


def failure_callback(context):
    """
    Combined failure callback that:
    1. Sends a Slack alert when a task fails.
    2. Updates the job status to 'failed' via an HTTP request.
    """
    dag_run: DagRun = context['dag_run']
    dag_run_id = dag_run.run_id if dag_run else "unknown"

    # 1. Send Slack Notification
    try:
        slack_msg = f"""
            :red_circle: Task Failed. Please go to Airflow for more details.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
        """
        slack_alert = SlackWebhookOperator(
            task_id='slack_failed_alert',
            slack_webhook_conn_id=SLACK_CONN_ID,
            message=slack_msg,
            username='airflow',
            channel=SLACK_CHANNEL
        )
        slack_alert.execute(context=context)
        print("Slack alert sent successfully.")
    except Exception as slack_error:
        print(f"Failed to send Slack alert: {slack_error}")

    # 2. Update Job Status to 'failed'
    try:
        fail_job_status = SimpleHttpOperator(
            task_id='update_job_status',
            http_conn_id='sarfinder',
            endpoint='api/sarfinder/airflow/task/update/',
            method='POST',
            headers={"Content-Type": "application/json"},
            data=json.dumps({
                "status": "failed",
                "dag_run_id": "{{ run_id }}"
            })
        )

        fail_job_status.execute(context=context)

        print(f"Updated status to failed for DAG run {dag_run_id}")
    except requests.RequestException as http_error:
        print(f"Failed to update job status: {http_error}")


def cleanup_variables(**kwargs):
    run_id = kwargs['run_id']
    Variable.delete(run_id)

def set_variables(**kwargs):
    run_id = kwargs['run_id']
    variables = Variable.get(f"{name}_variables")
    Variable.set(run_id, variables)


with DAG(
    dag_id=name,
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    on_failure_callback=failure_callback
) as dag:
    set_variable_task = PythonOperator(
        task_id='set_variables',
        python_callable=set_variables,
        provide_context=True
    )

    prepare_directory = SSHOperator(
        task_id="00a_prepare_directory_fpm3.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd("export VARIABLE=$(echo '{{ var.value[run_id] }}' | tr -d '\\n') && 00a_prepare_directory_fpm3.sh \"$VARIABLE\"", use_dir=False),
        cmd_timeout=None,
        conn_timeout=None
    )

    select_scenes = SSHOperator(
        task_id="01_fpm3_select_scenes.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('01_fpm3_select_scenes.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    update_download_config = SSHOperator(
        task_id="01a_update_download_config.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('01a_update_download_config.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    download = SSHOperator(
        task_id="01b_download.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('01b_download.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    create_run_files = SSHOperator(
        task_id="03_fpm3_create_run_files.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('03_fpm3_create_run_files.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    process = SSHOperator(
        task_id="04_auto_control.sh_start_run_fpm3_process.sh",
        ssh_conn_id='ssh',
        command=auto_control("run_fpm3_process"),
        cmd_timeout=None,
        conn_timeout=None
    )

    inference = SSHOperator(
        task_id="04_auto_control.sh_start_run_fpm3_inference.sh",
        ssh_conn_id='ssh',
        command=auto_control("run_fpm3_inference", gpu=True),
        cmd_timeout=None,
        conn_timeout=None
    )

    postprocess = SSHOperator(
        task_id="04_auto_control.sh_start_run_fpm3_post.sh",
        ssh_conn_id='ssh',
        command=auto_control("run_fpm3_post"),
        cmd_timeout=None,
        conn_timeout=None
    )

    send_slack = SlackWebhookOperator(
        task_id='send_slack_notifications',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=':blob_excited:On your MacBook, run the following scripts to download FPM3 products:blob_excited:\n```\nscp -r aws-hpc2:/home/ubuntu/urgent_response/{{ var.json[run_id].dir_name }}/fpm3/products .\n```\n \n',
        channel=SLACK_CHANNEL,
        username='airflow'
    )

    update_job_status = SimpleHttpOperator(
        task_id='update_job_status',
        http_conn_id='sarfinder',
        endpoint='api/sarfinder/airflow/task/update/',
        method='POST',
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "status": "success",
            "dag_run_id": "{{ run_id }}"
        }),
        extra_options={"check_response": False}  # Ignores HTTP errors
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_variables',
        python_callable=cleanup_variables,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS  # Ensures task runs only if all upstream tasks succeed
    )

    set_variable_task >> prepare_directory >> select_scenes >> update_download_config >> download
    download >> create_run_files >> process >> inference >> postprocess
    postprocess >> send_slack >> update_job_status >> cleanup_task
