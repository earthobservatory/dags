from airflow.decorators import dag
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow import DAG
from airflow.models import Variable
import json
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

with DAG(
    dag_id="DPM1",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    dir_name = Variable.get("DPM1_variables", deserialize_json=True)["dir_name"],
    #print(DPM1_variables)
    #dir_name = DPM1_variables["dir_name"],

    prepare_directory = SSHOperator(
        task_id="00a_prepare_directory_dpm1.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; 00a_prepare_directory_dpm1.sh "{{var.json.DPM1_variables}}"',
        cmd_timeout=None,
        conn_timeout=None
    )

    get_dem = SSHOperator(
        task_id="00_get_dem.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM1_variables.dir_name}}; 00_get_dem.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    update_download_config = SSHOperator(
        task_id="01a_update_download_config.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM1_variables.dir_name}}; 01a_update_download_config.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    download= SSHOperator(
        task_id="01b_download.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM1_variables.dir_name}}; 01b_download.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    symlink= SSHOperator(
        task_id="02a_symlink_data.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM1_variables.dir_name}}; 02a_symlink_data.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    # create_run_script= SSHOperator(
    #     task_id="03_create_run_script.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM1_variables.dir_name}}; 03_create_run_script.sh ""',
    #     cmd_timeout=None
    # )

    dpm1_response_setup= SSHOperator(
        task_id="03a_dpm1_response_setup.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM1_variables.dir_name}}; 03a_dpm1_response_setup.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    # symlink_orbits= SSHOperator(
    #     task_id="03b_symlink_orbits.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM1_variables.dir_name}}; 03b_symlink_orbits.sh ""',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    auto_control_run1= SSHOperator(
        task_id="04_auto_control.sh_start_run1.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM1_variables.dir_name}}; 04_auto_control.sh "test" "start" "run1" "run1"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2= SSHOperator(
        task_id="04_auto_control.sh_start_run2.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.DPM1_variables.dir_name}}; 04_auto_control.sh "test" "start" "run2" "run2"',
        cmd_timeout=None,
        conn_timeout=None
    )


    send_slack = SlackWebhookOperator(
    task_id='send_slack_notifications',
    slack_webhook_conn_id = 'slack_webhook_dpm1',
    message='On your MacBook, run the following scripts : \n \n \n To download DPM1 products : \n scp -r aws-hpc:/home/centos/urgent_response/{{var.json.DPM1_variables.dir_name}}/dpm1/\* . \n \n \n',
    channel='#dpm1-sarfinder-aws-hpc',
    username='airflow'
    )

    prepare_directory >> get_dem >> update_download_config >> download >> symlink >> dpm1_response_setup >> auto_control_run1 >> auto_control_run2 >> send_slack