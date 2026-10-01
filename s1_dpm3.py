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
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import DagRun
from airflow.utils.trigger_rule import TriggerRule
from copy import deepcopy
from airflow.operators.dummy import DummyOperator

import requests

name = "S1_DPM3"
SETUP_SCRIPT = "/home/ubuntu/insarscripts/stack_processor_aws/env_setup/setup_dpm3_aws.sh"

def ssh_cmd(script: str, use_dir: bool = True) -> str:
    base = "source " + SETUP_SCRIPT
    if use_dir:
        base += "; cd urgent_response/{{ var.json[run_id].dir_name }}"
    return base + "; " + script


def failure_callback(context):
    dag_run: DagRun = context['dag_run']
    dag_run_id = dag_run.run_id if dag_run else "unknown"

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
            channel='#dpm3-sarfinder-aws-hpc'
        )
        slack_alert.execute(context=context)
        print("Slack alert sent successfully.")
    except Exception as slack_error:
        print(f"Failed to send Slack alert: {slack_error}")

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
    except Exception as http_error:
        print(f"Failed to update job status: {http_error}")


def cleanup_variables(**kwargs):
    run_id = kwargs['run_id']
    Variable.delete(run_id)


def set_and_store_variables(**kwargs):
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
        task_id='set_and_store_variables',
        python_callable=set_and_store_variables,
        provide_context=True
    )

    prepare_directory = SSHOperator(
        task_id="00a_prepare_directory_dpm3.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd(
            'export VARIABLE=$(echo \'{{ var.value[run_id] }}\' | tr -d \'\\n\') && 00a_prepare_directory_dpm3.sh "$VARIABLE"',
            use_dir=False
        ),
        cmd_timeout=None,
        conn_timeout=None
    )

    get_dem = SSHOperator(
        task_id="00_get_dem_adv.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('00_get_dem_adv.sh ""'),
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

    symlink = SSHOperator(
        task_id="02a_symlink_data.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('02a_symlink_data.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    stackproc_runfile_setup = SSHOperator(
        task_id="03_create_run_script_xpm2.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('03_create_run_script_xpm2.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run1 = SSHOperator(
        task_id="04_auto_control.sh_start_run1.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run1" "run1"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2 = SSHOperator(
        task_id="04_auto_control.sh_start_run2.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run2" "run2"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2x5 = SSHOperator(
        task_id="04_auto_control.sh_start_run2x5.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2x5" "start" "run2x5" "run2x5"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run3 = SSHOperator(
        task_id="04_auto_control.sh_start_run3.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run3" "start" "run3" "run3"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run4 = SSHOperator(
        task_id="04_auto_control.sh_start_run4.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run4" "start" "run4" "run4"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run5 = SSHOperator(
        task_id="04_auto_control.sh_start_run5.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run5" "start" "run5" "run5"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run6 = SSHOperator(
        task_id="04_auto_control.sh_start_run6.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run6" "start" "run6" "run6"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run7 = SSHOperator(
        task_id="04_auto_control.sh_start_run7.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_run7" "start" "run7" "run7"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    branch_generate_ifg = BranchPythonOperator(
        task_id='branch_generate_ifg',
        python_callable=lambda **kwargs: '05i_generate_ifg' if Variable.get(kwargs['run_id'], deserialize_json=True).get('generate_ifg', False) else None,
        provide_context=True
    )

    generate_ifg = SSHOperator(
        task_id="05i_generate_ifg",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "{{ var.json[run_id].dir_name }}_runifg" "start" "run_ifg" "run_ifg"'),
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
        command=ssh_cmd('04_auto_control.sh "06_run_ccd_1" "start" "run_ccd_1" "run_ccd_1"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_2 = SSHOperator(
        task_id="06_run_ccd_2",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_ccd_2" "start" "run_ccd_2" "run_ccd_2"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_3 = SSHOperator(
        task_id="06_run_ccd_3",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_ccd_3" "start" "run_ccd_3" "run_ccd_3"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_4 = SSHOperator(
        task_id="06_run_ccd_4",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_ccd_4" "start" "run_ccd_4" "run_ccd_4"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_5 = SSHOperator(
        task_id="06_run_ccd_5",
        ssh_conn_id='ssh',
        command=ssh_cmd('rm -rf dpm3/probGV && 04_auto_control.sh "06_run_ccd_5" "start" "run_ccd_5" "run_ccd_5"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_6 = SSHOperator(
        task_id="06_run_ccd_6",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_ccd_6" "start" "run_ccd_6" "run_ccd_6"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_7 = SSHOperator(
        task_id="06_run_ccd_7",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_ccd_7" "start" "run_ccd_7" "run_ccd_7"'),
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run_ccd_8 = SSHOperator(
        task_id="06_run_ccd_8",
        ssh_conn_id='ssh',
        command=ssh_cmd('04_auto_control.sh "06_run_ccd_8" "start" "run_ccd_8" "run_ccd_8"'),
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

    apply_layovershadow = SSHOperator(
        task_id="06_apply_layovershadow.sh",
        ssh_conn_id='ssh',
        command=ssh_cmd('06_apply_layovershadow.sh ""'),
        cmd_timeout=None,
        conn_timeout=None
    )

    send_slack = SlackWebhookOperator(
        task_id='send_slack_notifications',
        slack_webhook_conn_id='slack_webhook_dpm3',
        message=':blob_excited:On your MacBook, run the following scripts to download DPM3 products:blob_excited:\n```\nscp -r aws-hpc2:/home/ubuntu/urgent_response/{{ var.json[run_id].dir_name }}/dpm3/probGV/\\*tif .\n```\n \n',
        channel='#dpm3-sarfinder-aws-hpc',
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
        })
    )

    # archive_task = SSHOperator(
    #     task_id='response_archive',
    #     ssh_conn_id='ssh',
    #     command=ssh_cmd('archive_responses.sh -f {{ var.json[run_id].dir_name }}'),
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    cleanup_task = PythonOperator(
        task_id='cleanup_variables',
        python_callable=cleanup_variables,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # ── DAG wiring ──────────────────────────────────────────────────────────────

    set_variable_task >> prepare_directory >> [get_dem, update_download_config]
    update_download_config >> download >> symlink
    [get_dem, symlink] >> stackproc_runfile_setup >> \
        auto_control_run1 >> auto_control_run2 >> auto_control_run2x5 >> auto_control_run3 >> \
        auto_control_run4 >> auto_control_run5 >> auto_control_run6 >> auto_control_run7

    auto_control_run7 >> branch_generate_ifg >> generate_ifg
    auto_control_run7 >> [generate_slcstk2cor_p1, generate_slcstk2icor_p2]

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

    run_dpm3_weighted_mean >> apply_layovershadow >> upload_greyscale  >> send_slack >> update_job_status >> cleanup_task