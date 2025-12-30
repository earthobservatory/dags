from airflow.decorators import dag
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow import DAG
from airflow.models import Variable
import json
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import DagRun
from airflow.utils.trigger_rule import TriggerRule

import requests

name="A2_DPM1"

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
            slack_webhook_conn_id='slack_webhook_dpm1',
            message=slack_msg,
            username='airflow',
            channel='#dpm1-sarfinder-aws-hpc'
        )
        slack_alert.execute(context=context)
        print("Slack alert sent successfully.")
    except Exception as slack_error:
        print(f"Failed to send Slack alert: {slack_error}")

    # 2. Update Job Status to 'failed'
    try:
        fail_job_status = SimpleHttpOperator(
            task_id='update_job_status',
            http_conn_id='sarfinder',  # Define this connection in Airflow
            endpoint='api/sarfinder/airflow/task/update/',  # Replace with your actual endpoint
            method='POST',
            headers={"Content-Type": "application/json"},
            data=json.dumps({
                # "request_id": "{{ var.json[run_id].request_id }}",  # Access run_id from XCom
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
        task_id="00a_prepare_directory_dpm1_alos2App.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; echo VARIABLES: "{{ var.json[run_id] }}"; 00a_prepare_directory_dpm1_alos2App.sh "{{ var.json[run_id] }}"',
        cmd_timeout=None,
        conn_timeout=None
    )

    runfile_setup = SSHOperator(
        task_id="03_create_run_script_alos_dpmx.sh",
        ssh_conn_id='ssh',
        command='source ~/insarscripts/stack_processor_aws/env_setup/setup_xpm_alos.sh; cd urgent_response/{{ var.json[run_id].dir_name }}; 03_create_run_script_alos_dpmx.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    
    auto_control_run1= SSHOperator(
        task_id="auto_control_run1",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd ~/urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run1" "run1"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2= SSHOperator(
        task_id="auto_control_run2",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run2" "run2"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run3= SSHOperator(
        task_id="auto_control_run3",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run3" "start" "run3" "run3"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run4= SSHOperator(
        task_id="auto_control_run4",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run4" "start" "run4" "run4"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run5= SSHOperator(
        task_id="auto_control_run5",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run4" "start" "run5" "run5"',
        cmd_timeout=None,
        conn_timeout=None
    )


    send_slack = SlackWebhookOperator(
        task_id='send_slack_notifications',
        slack_webhook_conn_id = 'slack_webhook_dpm1',
        message=':blob_excited:On your MacBook, run the following scripts to download DPM1 products:blob_excited:\n```\nscp -r aws-hpc2:/home/ubuntu/urgent_response/{{ var.json[run_id].dir_name }}/dpm1/\* .\n```\n \n',
        channel='#dpm1-sarfinder-aws-hpc',
        username='airflow'
    )


    update_job_status = SimpleHttpOperator(
        task_id='update_job_status',
        http_conn_id='sarfinder',  # Define this connection in Airflow
        endpoint='api/sarfinder/airflow/task/update/',  # Replace with your actual endpoint
        method='POST',
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            # "request_id": "{{ var.json[run_id].request_id }}",  # Access run_id from XCom
            "status": "success",
            "dag_run_id": "{{ run_id }}"
        }),
        extra_options={"check_response": False}  # Ignores HTTP errors
    )

    
    archive_task = SSHOperator(
        task_id='response_archive',
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; archive_responses.sh -f {{ var.json[run_id].dir_name }}',
        cmd_timeout=None,
        conn_timeout=None
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_variables',
        python_callable=cleanup_variables,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS  # Ensures task runs only if all upstream tasks succeed

    )
    
    

    set_variable_task >> prepare_directory >> runfile_setup >> \
    auto_control_run1 >> auto_control_run2 >> \
    auto_control_run3 >> auto_control_run4 >> \
    auto_control_run5 >> send_slack >> update_job_status >> archive_task >> cleanup_task
