from airflow.decorators import dag
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow import DAG
from airflow.models import Variable
import json
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook

def task_failure_alert(context):
    slack_webhook_token = BaseHook.get_connection('slack_webhook_dpm2').password
    slack_msg = f"""
        :red_circle: Task Failed. Please go to Airflow for more details.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('execution_date')}
        *Log Url*: {context.get('task_instance').log_url}
    """
    failed_alert = SlackWebhookOperator(
        task_id='slack_failed_alert',
        http_conn_id='slack_webhook_dpm2',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        channel='#dpm2-sarfinder-aws-hpc'
    )
    return failed_alert.execute(context=context)


with DAG(
    dag_id="DPM2",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    on_failure_callback=task_failure_alert
) as dag:
    dir_name = Variable.get("DPM2_variables", deserialize_json=True)["dir_name"],

    prepare_directory = SSHOperator(
        task_id="00a_prepare_directory_dpm2.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; 00a_prepare_directory_dpm2.sh "{{var.json.DPM2_variables}}"',
        cmd_timeout=None,
        conn_timeout=None
    )

    get_dem = SSHOperator(
        task_id="00_get_dem.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 00_get_dem.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    update_download_config = SSHOperator(
        task_id="01a_update_download_config.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 01a_update_download_config.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    download= SSHOperator(
        task_id="01b_download.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 01b_download.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    symlink= SSHOperator(
        task_id="02a_symlink_data.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 02a_symlink_data.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    dpm2_response_setup= SSHOperator(
        task_id="03_create_run_script.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 03_create_run_script.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    # symlink_orbits= SSHOperator(
    #     task_id="03b_symlink_orbits.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 03b_symlink_orbits.sh "{{var.json.DPM2_variables}}"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    auto_control_run1= SSHOperator(
        task_id="04_auto_control.sh_start_run1.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 04_auto_control.sh "{{var.json.DPM2_variables.dir_name}}_run1" "start" "run1" "run1"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2= SSHOperator(
        task_id="04_auto_control.sh_start_run2.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 04_auto_control.sh "{{var.json.DPM2_variables.dir_name}}_run2" "start" "run2" "run2"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2x5= SSHOperator(
        task_id="04_auto_control.sh_start_run2x5.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 04_auto_control.sh "{{var.json.DPM2_variables.dir_name}}_run2x5" "start" "run2x5" "run2x5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run3= SSHOperator(
        task_id="04_auto_control.sh_start_run3.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 04_auto_control.sh "{{var.json.DPM2_variables.dir_name}}_run3" "start" "run3" "run3"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run4= SSHOperator(
        task_id="04_auto_control.sh_start_run4.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 04_auto_control.sh "{{var.json.DPM2_variables.dir_name}}_run4" "start" "run4" "run4"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run5= SSHOperator(
        task_id="04_auto_control.sh_start_run5.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 04_auto_control.sh "{{var.json.DPM2_variables.dir_name}}_run5" "start" "run5" "run5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run6= SSHOperator(
        task_id="04_auto_control.sh_start_run6.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 04_auto_control.sh "{{var.json.DPM2_variables.dir_name}}_run6" "start" "run6" "run6"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run7= SSHOperator(
        task_id="04_auto_control.sh_start_run7.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 04_auto_control.sh "{{var.json.DPM2_variables.dir_name}}_run7" "start" "run7" "run7"',
        cmd_timeout=None,
        conn_timeout=None
    )

    generate_slcstk2cor= SSHOperator(
        task_id="05_generate_slcstk2cor.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM2_variables.dir_name}}; 05_generate_slcstk2cor.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )


    send_slack = SlackWebhookOperator(
        task_id='send_slack_notifications',
        slack_webhook_conn_id = 'slack_webhook_dpm2',
        message=':blob_excited:On your MacBook, run the following scripts to download DPM2 products:blob_excited:\n```\nscp -r aws-hpc:/home/centos/urgent_response/{{var.json.DPM2_variables.dir_name}}/dpm2/probGV/\*tif .\n```\n \n',
        channel='#dpm2-sarfinder-aws-hpc',
        username='airflow'
    )

    prepare_directory >> get_dem >> update_download_config >> download >> symlink >> dpm2_response_setup >> auto_control_run1 >> auto_control_run2 >> auto_control_run2x5 >> auto_control_run3 >> auto_control_run4 >> auto_control_run5 >> auto_control_run6 >> auto_control_run7 >> generate_slcstk2cor >> send_slack

    #prepare_directory >> get_dem >> update_download_config >> download >> symlink >> dpm2_response_setup >> auto_control_run1 >> generate_slcstk2cor >> send_slack