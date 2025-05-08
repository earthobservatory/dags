from airflow.decorators import dag
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import BranchPythonOperator
from airflow import DAG
from airflow.models import Variable
import json
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import DagRun
from airflow.utils.trigger_rule import TriggerRule
from copy import deepcopy


import requests
name = "DPM2A2_C2"


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


def set_and_store_variables(**kwargs):
    run_id = kwargs['run_id']
    variables = Variable.get(f"{name}_variables")
    Variable.set(run_id, variables)

with (DAG(
        dag_id=name,
        schedule_interval=None,
        start_date=datetime(2022, 1, 1),
        catchup=False,
        on_failure_callback=failure_callback
) as dag):
    set_variable_task = PythonOperator(
        task_id='set_and_store_variables',
        python_callable=set_and_store_variables,
        provide_context=True
    )

    prepare_directory = SSHOperator(
        task_id="00a_prepare_directory_dpm2_alosStack.sh",
        ssh_conn_id='ssh_eosaws2',
#        command=f'source ~/.bash_profile; echo VARIABLES: {json.dumps({{ var.value[run_id] }})}; 00a_prepare_directory_dpm2.sh {json.dumps({{ var.value[run_id] }})}',
        command="source ~/insarscripts/stack_processor_aws/env_setup/setup_xpm_alos.sh; export VARIABLE=$(echo '{{ var.value[run_id] }}' | tr -d '\n')  &&  00a_prepare_directory_dpm2_alosStack.sh \"$VARIABLE\"",
        cmd_timeout=None,
        conn_timeout=None
    )

    symlink = SSHOperator(
        task_id="02a_symlink_data_alosStack.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/insarscripts/stack_processor_aws/env_setup/setup_xpm_alos.sh; cd urgent_response/{{ var.json[run_id].dir_name }}; 02a_symlink_data_alosStack.sh ""', 
        cmd_timeout=None,
        conn_timeout=None
    )

    stackproc_runfile_setup = SSHOperator(
        task_id="03_create_run_script_alos_dpmx.sh",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/insarscripts/stack_processor_aws/env_setup/setup_xpm_alos.sh; cd urgent_response/{{ var.json[run_id].dir_name }}; 03_create_run_script_alos_dpmx.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    run01a = SSHOperator(
        task_id="run01a",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run01a" "run01a"',
        cmd_timeout=None,
        conn_timeout=None
    )

    run01b = SSHOperator(
        task_id="run01b",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run01b" "run01b"',
        cmd_timeout=None,
        conn_timeout=None
    )
    run01c = SSHOperator(
        task_id="run01c",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run01c" "run01c"',
        cmd_timeout=None,
        conn_timeout=None
    )
    run01d = SSHOperator(
        task_id="run01d",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run01d" "run01d"',
        cmd_timeout=None,
        conn_timeout=None
    )

    run01e = SSHOperator(
        task_id="run01e",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run01e" "run01e"',
        cmd_timeout=None,
        conn_timeout=None
    )
    run01f = SSHOperator(
        task_id="run01f",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh  "{{ var.json[run_id].dir_name }}_run1" "start" "run01f" "run01f"',
        cmd_timeout=None,
        conn_timeout=None
    )

    run02a = SSHOperator(
        task_id="run02a",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02a" "run02a"',
        cmd_timeout=None,
        conn_timeout=None
    )

    run02b = SSHOperator(
        task_id="run02b",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02b" "run02b"',
        cmd_timeout=None,
        conn_timeout=None
    )

    run02c = SSHOperator(
        task_id="run02c",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02c" "run02c"',
        cmd_timeout=None,
        conn_timeout=None
    )

    run02d = SSHOperator(
        task_id="run02d",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02d" "run02d"',
        cmd_timeout=None,
        conn_timeout=None
    )
    run02e = SSHOperator(
        task_id="run02e",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02e" "run02e"',
        cmd_timeout=None,
        conn_timeout=None
    )
    run02f = SSHOperator(
        task_id="run02f",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02f" "run02f"',
        cmd_timeout=None,
        conn_timeout=None
    )
    run02g = SSHOperator(
        task_id="run02g",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02g" "run02g"',
        cmd_timeout=None,
        conn_timeout=None
    )

    generate_slcstk2cor = SSHOperator(
        task_id="05a_generate_slcstk2cor.sh",
        ssh_conn_id='ssh_eosaws2',
#        command='source ~/.bash_profile; which rilooks',
        command='source ~/insarscripts/stack_processor_aws/env_setup/setup_xpm_alos.sh; cd urgent_response/{{ var.json[run_id].dir_name }}; 05_generate_slcstk2cor_alosStack.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    create_dpm2_runfiles = SSHOperator(
        task_id="05b_create_dpm2_runfiles",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 05b_create_dpm2_runfiles.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_1 = SSHOperator(
        task_id="06_run_dpm2_1",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_1" "start" "run_dpm2_1" "run_dpm2_1"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_2 = SSHOperator(
        task_id="06_run_dpm2_2",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_2" "start" "run_dpm2_2" "run_dpm2_2"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_3 = SSHOperator(
        task_id="06_run_dpm2_3",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_3" "start" "run_dpm2_3" "run_dpm2_3"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_4 = SSHOperator(
        task_id="06_run_dpm2_4",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_4" "start" "run_dpm2_4" "run_dpm2_4"',
        cmd_timeout=None,
        conn_timeout=None,
        trigger_rule=TriggerRule.ONE_SUCCESS,

    )
    auto_control_run_dpm2_5 = SSHOperator(
        task_id="06_run_dpm2_5",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; rm -rf dpm2/probGV; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_5" "start" "run_dpm2_5" "run_dpm2_5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_6 = SSHOperator(
        task_id="06_run_dpm2_6",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_6" "start" "run_dpm2_6" "run_dpm2_6"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_7 = SSHOperator(
        task_id="06_run_dpm2_7",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_7" "start" "run_dpm2_7" "run_dpm2_7"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_dpm2_8 = SSHOperator(
        task_id="06_run_dpm2_8",
        ssh_conn_id='ssh_eosaws2',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_8" "start" "run_dpm2_8" "run_dpm2_8"',
        cmd_timeout=None,
        conn_timeout=None
    )


    cleanup_task = PythonOperator(
        task_id='cleanup_variables',
        python_callable=cleanup_variables,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS  # Ensures task runs only if all upstream tasks succeed

    )

    send_slack = SlackWebhookOperator(
        task_id='send_slack_notifications',
        slack_webhook_conn_id='slack_webhook_dpm2',
        message=':blob_excited:On your MacBook, run the following scripts to download DPM2 products:blob_excited:\n```\nscp -r aws-hpc:/home/ubuntu/urgent_response/{{ var.json[run_id].dir_name }}/dpm2/probGV/\*tif .\n```\n \n',
        channel='#dpm2-sarfinder-aws-hpc',
        username='airflow'
    )

    set_variable_task >> prepare_directory >>  symlink >> stackproc_runfile_setup >> \
    run01a >> run01b >> run01c >> run01d >> run01e >> run01f >> \
    run02a >> run02b >> run02c >> run02d >> run02e >> run02f >> run02g >> \
    generate_slcstk2cor >> create_dpm2_runfiles >> auto_control_run_dpm2_1 >> auto_control_run_dpm2_2 >> \
    auto_control_run_dpm2_3 >> auto_control_run_dpm2_4 >> auto_control_run_dpm2_5 >> \
    auto_control_run_dpm2_6 >> auto_control_run_dpm2_7 >> auto_control_run_dpm2_8 >> \
    send_slack >> cleanup_task


    # Break here to wait, use a Deferring Task (Sensor etc) to check for variable change
    # Do:
    # update_selection_file
    # download
    # symlink
    # stackproc_runfile_setup_update
    # auto_control_runu1 to auto_control_runu6
    # generate_slcstk2cor (no need update)
    # auto_control_run_dpm2_4 >> auto_control_run_dpm2_5 >> auto_control_run_dpm2_6 >> auto_control_run_dpm2_7 >> auto_control_run_dpm2_8 >> \
    # send_slack >> upload_greyscale >> update_job_status >> cleanup_task
