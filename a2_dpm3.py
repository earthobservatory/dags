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
name = "A2_DPM3"

SETUP_SCRIPT = "/home/ubuntu/insarscripts/stack_processor_aws/env_setup/setup_dpm3_alos_aws.sh"


def ssh_cmd(script: str, use_dir: bool = True) -> str:
    base = "source " + SETUP_SCRIPT
    if use_dir:
        base += "; cd urgent_response/{{ var.json[run_id].dir_name }}"
    return base + "; " + script


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
            slack_webhook_conn_id='slack_webhook_dpm3',
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
        task_id="00a_prepare_directory_dpm3_alosStack.sh",
        ssh_conn_id='ssh',
#        command=f'source ~/.bash_profile; echo VARIABLES: {json.dumps({{ var.value[run_id] }})}; 00a_prepare_directory_dpm2.sh {json.dumps({{ var.value[run_id] }})}',
        command=ssh_cmd("export VARIABLE=$(echo '{{ var.value[run_id] }}' | tr -d '\\n') && 00a_prepare_directory_dpm3_alosStack.sh \"$VARIABLE\"", use_dir=False),
        cmd_timeout=None,
        conn_timeout=None
    )

    symlink = SSHOperator(
        task_id="02a_symlink_data_alosStack.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('02a_symlink_data_alosStack.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    stackproc_runfile_setup = SSHOperator(
        task_id="03_create_run_script_alos_dpmx.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('03_create_run_script_alos_dpmx.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run01a = SSHOperator(
        task_id="run01a",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run01a" "run01a"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run01b = SSHOperator(
        task_id="run01b",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run01b" "run01b"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run01c = SSHOperator(
        task_id="run01c",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run01c" "run01c"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run01d = SSHOperator(
        task_id="run01d",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run01d" "run01d"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run01e = SSHOperator(
        task_id="run01e",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run01e" "run01e"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run01f = SSHOperator(
        task_id="run01f",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh  "{{ var.json[run_id].dir_name }}_run1" "start" "run01f" "run01f"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run02a = SSHOperator(
        task_id="run02a",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02a" "run02a"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run02b = SSHOperator(
        task_id="run02b",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02b" "run02b"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run02c = SSHOperator(
        task_id="run02c",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02c" "run02c"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run02d = SSHOperator(
        task_id="run02d",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02d" "run02d"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run02e = SSHOperator(
        task_id="run02e",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02e" "run02e"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run02f = SSHOperator(
        task_id="run02f",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02f" "run02f"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run02g = SSHOperator(
        task_id="run02g",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run02g" "run02g"'),
        cmd_timeout=None,
        conn_timeout=None
    )


    normalize_alos = SSHOperator(
        task_id="04b_normalize_alos.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('04b_normalize_alos.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )


    generate_slcstk2cor_p1 = SSHOperator(
        task_id="05a_p1_generate_slcstk2cor.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('05a_p1_generate_slcstk2cor.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    generate_slcstk2icor_p2 = SSHOperator(
        task_id="05a_p2_generate_slcstk2icor.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('05a_p2_generate_slcstk2icor.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    create_dpm3_cor_runfiles_p1 = SSHOperator(
        task_id="05b_p1_create_dpm3_cor_runfiles",
        ssh_conn_id='ssh',
        command=ssh_cmd('05b_p1_create_dpm3_cor_runfiles.sh ""'),
        cmd_timeout=None,
        conn_timeout=None,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    auto_control_run_ccd_1 = SSHOperator(
        task_id="06_run_ccd_1",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_ccd_1" "start" "run_ccd_1" "run_ccd_1"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_2 = SSHOperator(
        task_id="06_run_ccd_2",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_ccd_2" "start" "run_ccd_2" "run_ccd_2"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_3 = SSHOperator(
        task_id="06_run_ccd_3",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_ccd_3" "start" "run_ccd_3" "run_ccd_3"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_4 = SSHOperator(
        task_id="06_run_ccd_4",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_ccd_4" "start" "run_ccd_4" "run_ccd_4"'),
        cmd_timeout=None,
        conn_timeout=None,
        trigger_rule=TriggerRule.ONE_SUCCESS,

    )

    auto_control_run_ccd_5 = SSHOperator(
        task_id="06_run_ccd_5",
        ssh_conn_id='ssh',
        command=ssh_cmd('rm -rf ccd/probGV; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_ccd_5" "start" "run_ccd_5" "run_ccd_5"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_6 = SSHOperator(
        task_id="06_run_ccd_6",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_ccd_6" "start" "run_ccd_6" "run_ccd_6"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_7 = SSHOperator(
        task_id="06_run_ccd_7",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_ccd_7" "start" "run_ccd_7" "run_ccd_7"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_8 = SSHOperator(
        task_id="06_run_ccd_8",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_ccd_8" "start" "run_ccd_8" "run_ccd_8"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    create_dpm3_icor_runfiles_p2 = SSHOperator(
        task_id="05b_p2_create_dpm3_icor_runfiles",
        ssh_conn_id='ssh',
        command=ssh_cmd('05b_p2_create_dpm3_icor_runfiles.sh ""'),
        cmd_timeout=None,
        conn_timeout=None,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    auto_control_run_icor_1 = SSHOperator(
        task_id="06_run_icor_1",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_icor_1" "start" "run_icor_1" "run_icor_1"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_icor_2 = SSHOperator(
        task_id="06_run_icor_2",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_icor_2" "start" "run_icor_2" "run_icor_2"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_icor_3 = SSHOperator(
        task_id="06_run_icor_3",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_icor_3" "start" "run_icor_3" "run_icor_3"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_icor_4 = SSHOperator(
        task_id="06_run_icor_4",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_icor_4" "start" "run_icor_4" "run_icor_4"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_icor_5 = SSHOperator(
        task_id="06_run_icor_5",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_icor_5" "start" "run_icor_5" "run_icor_5"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    run_dpm3_weighted_mean = SSHOperator(
        task_id="07_run_dpm3_weighted_mean.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('07_run_dpm3_weighted_mean.sh ""'),
        cmd_timeout=None,
        conn_timeout=None,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )


    upload_greyscale = SSHOperator(
        task_id="08_upload_greyscale.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('08_upload_greyscale.sh "{{ var.json[run_id].dir_name }}"'),
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
        message=':blob_excited:On your MacBook, run the following scripts to download DPM3 products:blob_excited:\n```\nscp -r aws-hpc2:/home/ubuntu/urgent_response/{{ var.json[run_id].dir_name }}/dpm3/probGV/\\*tif .\n```\n \n',
        channel='#dpm2-sarfinder-aws-hpc',
        username='airflow'
    )

    set_variable_task >> prepare_directory >>  symlink >> stackproc_runfile_setup >> \
    run01a >> run01b >> run01c >> run01d >> run01e >> run01f >> \
    run02a >> run02b >> run02c >> run02d >> run02e >> run02f >> run02g >> normalize_alos >> \
   [generate_slcstk2cor_p1, generate_slcstk2icor_p2]

    # CCD (cor) branch — unchanged
    [generate_slcstk2cor_p1, generate_slcstk2icor_p2] >> create_dpm3_cor_runfiles_p1
    create_dpm3_cor_runfiles_p1 >> \
        auto_control_run_ccd_1 >> auto_control_run_ccd_2 >> auto_control_run_ccd_3 >> \
        auto_control_run_ccd_4 >> auto_control_run_ccd_5 >> auto_control_run_ccd_6 >> \
        auto_control_run_ccd_7 >> auto_control_run_ccd_8

    # ICOR branch — now split into individual steps
    [generate_slcstk2cor_p1, generate_slcstk2icor_p2] >> create_dpm3_icor_runfiles_p2
    create_dpm3_icor_runfiles_p2 >> \
        auto_control_run_icor_1 >> auto_control_run_icor_2 >> auto_control_run_icor_3 >> \
        auto_control_run_icor_4 >> auto_control_run_icor_5

    # Both branches must finish before weighted mean
    [auto_control_run_ccd_8, auto_control_run_icor_5] >> run_dpm3_weighted_mean

    run_dpm3_weighted_mean >> upload_greyscale >> \
    send_slack >> cleanup_task
