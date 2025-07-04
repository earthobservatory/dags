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

name="FPM2_C2"


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
            slack_webhook_conn_id='slack_webhook_fpm2',
            message=slack_msg,
            username='airflow',
            channel='#fpm2-sarfinder-aws-hpc'
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
        task_id="00a_prepare_directory_fpm2.sh",
        ssh_conn_id='ssh_eosaws2',
        command="source ~/.bash_profile; export VARIABLE=$(echo '{{ var.value[run_id] }}' | tr -d '\n')  && 00a_prepare_directory_fpm2.sh \"$VARIABLE\"",
        cmd_timeout=None,
        conn_timeout=None
    )

    get_dem = SSHOperator(
        task_id="00_get_dem_adv.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 00_get_dem_adv.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    update_download_config = SSHOperator(
        task_id="01a_update_download_config.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 01a_update_download_config.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    download= SSHOperator(
        task_id="01b_download.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 01b_download.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    symlink= SSHOperator(
        task_id="02a_symlink_data.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 02a_symlink_data.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    dpm2_response_setup= SSHOperator(
        task_id="03_create_run_script_xpm2.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 03_create_run_script_xpm2.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run1= SSHOperator(
        task_id="04_auto_control.sh_start_run1.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 04_auto_control.sh "{{var.json[run_id].dir_name}}_run1" "start" "run1" "run1"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2= SSHOperator(
        task_id="04_auto_control.sh_start_run2.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 04_auto_control.sh "{{var.json[run_id].dir_name}}_run2" "start" "run2" "run2"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2x5= SSHOperator(
        task_id="04_auto_control.sh_start_run2x5.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 04_auto_control.sh "{{var.json[run_id].dir_name}}_run2x5" "start" "run2x5" "run2x5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run3= SSHOperator(
        task_id="04_auto_control.sh_start_run3.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 04_auto_control.sh "{{var.json[run_id].dir_name}}_run3" "start" "run3" "run3"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run4= SSHOperator(
        task_id="04_auto_control.sh_start_run4.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 04_auto_control.sh "{{var.json[run_id].dir_name}}_run4" "start" "run4" "run4"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run5= SSHOperator(
        task_id="04_auto_control.sh_start_run5.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 04_auto_control.sh "{{var.json[run_id].dir_name}}_run5" "start" "run5" "run5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run6= SSHOperator(
        task_id="04_auto_control.sh_start_run6.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 04_auto_control.sh "{{var.json[run_id].dir_name}}_run6" "start" "run6" "run6"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run7= SSHOperator(
        task_id="04_auto_control.sh_start_run7.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 04_auto_control.sh "{{var.json[run_id].dir_name}}_run7" "start" "run7" "run7"',
        cmd_timeout=None,
        conn_timeout=None
    )

    post_run6_geocode_series= SSHOperator(
        task_id="05_post_run6_geocode_series.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 05_post_run6_geocode_series.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    merge_fpm2_geo_files = SSHOperator(
    task_id="07_merge_fpm2_geo_files.sh",
    ssh_conn_id='ssh_eosaws2',
    command='source ~/.bash_profile; cd urgent_response/{{var.json[run_id].dir_name}}; 07_merge_fpm2_geo_files.sh ""',
    cmd_timeout=None,
    conn_timeout=None
    )


    send_slack = SlackWebhookOperator(
        task_id='send_slack_notifications',
        slack_webhook_conn_id = 'slack_webhook_fpm2',
        message=':blob_excited:On your MacBook, run the following scripts to download FPM2 products:blob_excited:\n```\nscp -r aws-hpc2:/home/ubuntu/urgent_response/{{var.json[run_id].dir_name}}/fpm2/fpm2.tar .\n```\n \n',
        channel='#fpm2-sarfinder-aws-hpc',
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


    
    cleanup_task = PythonOperator(
        task_id='cleanup_variables',
        python_callable=cleanup_variables,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS  # Ensures task runs only if all upstream tasks succeed

    )
    
    


    set_variable_task >> prepare_directory >> [get_dem, update_download_config]
    update_download_config >> download >> symlink
    [get_dem, symlink] >> dpm2_response_setup >> auto_control_run1 >> auto_control_run2 >> auto_control_run2x5 >> auto_control_run3 >> auto_control_run4 >> auto_control_run5 >> auto_control_run6 >> auto_control_run7 >> post_run6_geocode_series >> merge_fpm2_geo_files >> send_slack >>  update_job_status  >> cleanup_task
