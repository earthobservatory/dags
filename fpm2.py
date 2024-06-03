from airflow.decorators import dag
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow import DAG
from airflow.models import Variable
import json
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

with DAG(
    dag_id="FPM2",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    dir_name = Variable.get("FPM2_variables", deserialize_json=True)["dir_name"],

    prepare_directory = SSHOperator(
        task_id="00a_prepare_directory_fpm2.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; 00a_prepare_directory_fpm2.sh "{{var.json.FPM2_variables}}"',
        cmd_timeout=None,
        conn_timeout=None
    )

    get_dem = SSHOperator(
        task_id="00_get_dem.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 00_get_dem.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    update_download_config = SSHOperator(
        task_id="01a_update_download_config.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 01a_update_download_config.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    download= SSHOperator(
        task_id="01b_download.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 01b_download_airflow.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    symlink= SSHOperator(
        task_id="02a_symlink_data.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 02a_symlink_data.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    dpm2_response_setup= SSHOperator(
        task_id="03_create_run_script.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 03_create_run_script.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run1= SSHOperator(
        task_id="04_auto_control.sh_start_run1.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 04_auto_control_airflow.sh "test" "start" "run1" "run1"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2= SSHOperator(
        task_id="04_auto_control.sh_start_run2.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 04_auto_control_airflow.sh "test" "start" "run2" "run2"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2x5= SSHOperator(
        task_id="04_auto_control.sh_start_run2x5.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 04_auto_control_airflow.sh "test" "start" "run2x5" "run2x5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run3= SSHOperator(
        task_id="04_auto_control.sh_start_run3.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 04_auto_control_airflow.sh "test" "start" "run3" "run3"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run4= SSHOperator(
        task_id="04_auto_control.sh_start_run4.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 04_auto_control_airflow.sh "test" "start" "run4" "run4"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run5= SSHOperator(
        task_id="04_auto_control.sh_start_run5.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 04_auto_control_airflow.sh "test" "start" "run5" "run5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run6= SSHOperator(
        task_id="04_auto_control.sh_start_run6.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 04_auto_control_airflow.sh "test" "start" "run6" "run6"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run7= SSHOperator(
        task_id="04_auto_control.sh_start_run7.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 04_auto_control_airflow.sh "test" "start" "run7" "run7"',
        cmd_timeout=None,
        conn_timeout=None
    )

    post_run6_geocode_series= SSHOperator(
        task_id="post_run6_geocode_series.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; post_run6_geocode_series.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    merge_fpm2_geo_files = SSHOperator(
    task_id="07_merge_fpm2_geo_files.sh",
    ssh_conn_id='ssh',
    command='source ~/.bash_profile; cd urgent_response/{{var.json.FPM2_variables.dir_name}}; 07_merge_fpm2_geo_files.sh ""',
    cmd_timeout=None,
    conn_timeout=None
    )


    send_slack = SlackWebhookOperator(
        task_id='send_slack_notifications',
        slack_webhook_conn_id = 'slack_webhook_fpm2',
        message='On your MacBook, run the following scripts : \n \n \n To download FPM2 products : \n scp -r aws-hpc:/home/centos/urgent_response/{{var.json.FPM2_variables.dir_name}}/stack.tar . ',
        channel='#dpm2-sarfinder-aws-hpc',
        username='airflow'
    )

    prepare_directory >> get_dem >> update_download_config >> download >> symlink >> dpm2_response_setup >> auto_control_run1 >> auto_control_run2 >> auto_control_run2x5 >> auto_control_run3 >> auto_control_run4 >> auto_control_run5 >> auto_control_run6 >> auto_control_run7 >> post_run6_geocode_series >> merge_fpm2_geo_files >> send_slack