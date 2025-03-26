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

name="DPM2_BD"


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
            slack_webhook_conn_id='slack_webhook_dpm2',
            message=slack_msg,
            username='airflow',
            channel='#dpm2-sarfinder-aws-hpc'
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
    except Exception as http_error:
        print(f"Failed to update job status: {http_error}")
        

# def failure_callback(context):
#     """
#     Callback function to update job status to 'failed' when a task fails.
#     """
#     dag_run: DagRun = context['dag_run']
#     dag_run_id = dag_run.run_id if dag_run else "unknown"

#     payload = json.dumps({"status": "failed", "dag_run_id": dag_run_id})
    
#     headers = {"Content-Type": "application/json"}
    
#     try:
#         response = requests.post("http://sarfinder/api/sarfinder/airflow/task/update/", 
#                                  data=payload, headers=headers)
#         response.raise_for_status()
#         print(f"Updated status to failed for DAG run {dag_run_id}: {response.text}")
#     except requests.RequestException as e:
#         print(f"Failed to update job status: {e}")

def cleanup_variables(**kwargs):
    run_id = kwargs['run_id']
    Variable.delete(run_id)

def set_variables(**kwargs):
    run_id = kwargs['run_id']
    variables = Variable.get(f"{name}_variables")
    Variable.set(run_id, variables)



with ((DAG(
    dag_id=name,
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    on_failure_callback=failure_callback
) as dag)):
    set_variable_task = PythonOperator(
        task_id='set_variables',
        python_callable=set_variables,
        provide_context=True
    )
    
    prepare_directory = SSHOperator(
        task_id="00a_prepare_directory_dpm2.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; 00a_prepare_directory_dpm2.sh "{{ var.json[run_id] }}"',
        cmd_timeout=None,
        conn_timeout=None
    )

    get_dem = SSHOperator(
        task_id="00_get_dem_adv.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 00_get_dem_adv.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    update_download_config = SSHOperator(
        task_id="01a_update_download_config.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 01a_update_download_config.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    download= SSHOperator(
        task_id="01b_download.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 01b_download.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    symlink= SSHOperator(
        task_id="02a_symlink_data.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 02a_symlink_data.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    dpm2_response_setup= SSHOperator(
        task_id="03_create_run_script.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 03_create_run_script.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    # symlink_orbits= SSHOperator(
    #     task_id="03b_symlink_orbits.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 03b_symlink_orbits.sh "{variables}"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    auto_control_run1= SSHOperator(
        task_id="04_auto_control.sh_start_run1.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run1" "run1"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2= SSHOperator(
        task_id="04_auto_control.sh_start_run2.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run2" "run2"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2x5= SSHOperator(
        task_id="04_auto_control.sh_start_run2x5.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2x5" "start" "run2x5" "run2x5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run3= SSHOperator(
        task_id="04_auto_control.sh_start_run3.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run3" "start" "run3" "run3"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run4= SSHOperator(
        task_id="04_auto_control.sh_start_run4.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run4" "start" "run4" "run4"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run5= SSHOperator(
        task_id="04_auto_control.sh_start_run5.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run5" "start" "run5" "run5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run6= SSHOperator(
        task_id="04_auto_control.sh_start_run6.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run6" "start" "run6" "run6"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run7= SSHOperator(
        task_id="04_auto_control.sh_start_run7.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run7" "start" "run7" "run7"',
        cmd_timeout=None,
        conn_timeout=None
    )

    generate_slcstk2cor= SSHOperator(
        task_id="05a_generate_slcstk2cor.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 05a_generate_slcstk2cor.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    create_dpm2_runfiles= SSHOperator(
        task_id="05b_create_dpm2_runfiles",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 05b_create_dpm2_runfiles.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_1= SSHOperator(
        task_id="06_run_dpm2_1",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_1" "start" "run_dpm2_1" "run_dpm2_1"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_2= SSHOperator(
        task_id="06_run_dpm2_2",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_2" "start" "run_dpm2_2" "run_dpm2_2"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_3= SSHOperator(
        task_id="06_run_dpm2_3",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_3" "start" "run_dpm2_2" "run_dpm2_3"',
        cmd_timeout=None,
        conn_timeout=None
    )
    auto_control_run_dpm2_4= SSHOperator(
        task_id="06_run_dpm2_4",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_4" "start" "run_dpm2_4" "run_dpm2_4"',
        cmd_timeout=None,
        conn_timeout=None
    )
    auto_control_run_dpm2_5= SSHOperator(
        task_id="06_run_dpm2_5",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; rm -rf dpm2/probGV; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_5" "start" "run_dpm2_5" "run_dpm2_5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_6= SSHOperator(
        task_id="06_run_dpm2_6",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_6" "start" "run_dpm2_6" "run_dpm2_6"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_7= SSHOperator(
        task_id="06_run_dpm2_7",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_7" "start" "run_dpm2_7" "run_dpm2_7"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_8= SSHOperator(
        task_id="06_run_dpm2_8",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_8" "start" "run_dpm2_8" "run_dpm2_8"',
        cmd_timeout=None,
        conn_timeout=None
    )

    upload_greyscale= SSHOperator(
        task_id="08_upload_greyscale.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 08_upload_greyscale.sh "{{ var.json[run_id].dir_name }}" ',
        cmd_timeout=None,
        conn_timeout=None
    )


    send_slack = SlackWebhookOperator(
        task_id='send_slack_notifications',
        slack_webhook_conn_id = 'slack_webhook_dpm2',
        message=':blob_excited:On your MacBook, run the following scripts to download DPM2 products:blob_excited:\n```\nscp -r aws-hpc:/home/centos/urgent_response/{{ var.json[run_id].dir_name }}/dpm2/probGV/\*tif .\n```\n \n',
        channel='#dpm2-sarfinder-aws-hpc',
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
        })
    )


    
    cleanup_task = PythonOperator(
        task_id='cleanup_variables',
        python_callable=cleanup_variables,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS  # Ensures task runs only if all upstream tasks succeed

    )
 

    set_variable_task >> prepare_directory  >> update_download_config >> download >> symlink >> dpm2_response_setup >> \
    auto_control_run1 >> auto_control_run2 >> auto_control_run2x5 >> auto_control_run3 >> auto_control_run4 >> \
    auto_control_run5 >> auto_control_run6 >> auto_control_run7 >> generate_slcstk2cor >> \
    create_dpm2_runfiles >> auto_control_run_dpm2_1 >> auto_control_run_dpm2_2 >> auto_control_run_dpm2_3 >> auto_control_run_dpm2_4 >> \
    auto_control_run_dpm2_5 >> auto_control_run_dpm2_6 >> auto_control_run_dpm2_7 >> auto_control_run_dpm2_8 >> \
    upload_greyscale >> send_slack >> update_job_status >> cleanup_task

    #prepare_directory >> get_dem >> update_download_config >> download >> symlink >> dpm2_response_setup >> auto_control_run1 >> generate_slcstk2cor >> send_slack