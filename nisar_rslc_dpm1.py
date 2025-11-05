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
from airflow.operators.dummy import DummyOperator


import requests
name = "NI_RSLC_DPM1"


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
    prevent_selection_files = Variable.get(run_id, deserialize_json=True).get('selection_download', None)
    return prevent_selection_files

def check_postevent_ready(**kwargs):
    run_id = kwargs['run_id']
    ti = kwargs['ti']
    prevent_selection_files = ti.xcom_pull(task_ids='set_and_store_variables')
    current_selection_files = Variable.get(run_id, deserialize_json=True)['selection_download']
    post_event_scenes = list(set(current_selection_files) - set(prevent_selection_files))
    post_event_selection_str = "\n".join(post_event_scenes)
    print(f"post_event_scenes: {post_event_selection_str}")
    ti.xcom_push(key='post_event_selection_str', value=post_event_selection_str)
    return post_event_scenes

def choose_postevent_branch(**kwargs):
    run_id = kwargs['run_id']
    postevent_exists = Variable.get(run_id, deserialize_json=True)['postevent_exists']
    if postevent_exists:
        return 'continue_run_if_one_success'
    else:
        return 'wait_for_postevent'

def run_if_one_success(**context):
    dag_run = context["dag_run"]
    run_id = context['run_id']
    ti = context['ti']
    postevent_exists_branch = "wait_for_postevent" not in ti.xcom_pull(task_ids='check_postevent_branch')
    pre_dpm2_3_status = dag_run.get_task_instance("check_postevent_branch").state

    post_event_update_ti = dag_run.get_task_instance("05a_generate_slcstk2cor_update.sh")
    post_event_updatemode_state = post_event_update_ti.state if post_event_update_ti else None
    #post_event_updatemode_state = dag_run.get_task_instance("05a_generate_slcstk2cor_update.sh").state

    # Cases:
    # PASS: postevent scene does not exist >> 05a_generate_slcstk2cor_update.sh succeed /  postevent scene exist >> check_postevent_branch succeed (upto dpm2_3)
    # FAIL: postevent scene does not exist >> 05a_generate_slcstk2cor_update.sh fails / postevent scene exist >> check_postevent_branch upstream failed (upto dpm2_3)

    print(f"Upstream task: postevent_exists:{postevent_exists_branch} check_postevent_branch:{pre_dpm2_3_status} 05a_generate_slcstk2cor_update.sh:{post_event_updatemode_state}")

    if postevent_exists_branch and 'success' in pre_dpm2_3_status:
        print("Post-event exists and all runs succeeded continuing dpm2_processing.")
    elif not postevent_exists_branch and 'success' in post_event_updatemode_state:
        print("Post-event upate mode activated and all update mode succeeded continuing dpm2_processing.")
    else:
        raise Exception("No upstream task succeeded. Failing this task.")

def should_generate_ifg_branch(**kwargs):
    run_id = kwargs['run_id']
    variables = Variable.get(run_id, deserialize_json=True)
    if variables.get('generate_ifg', False):
        return '05i_generate_ifg'
    else:
        return None  # This will be a dummy branch for skipping


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
        task_id="00a_prepare_directory_dpm1.sh",
        ssh_conn_id='ssh',
#        command=f'source ~/.bash_profile; echo VARIABLES: {json.dumps({{ var.value[run_id] }})}; 00a_prepare_directory_dpm2.sh {json.dumps({{ var.value[run_id] }})}',
        command="source ~/.bash_profile; export VARIABLE=$(echo '{{ var.value[run_id] }}' | tr -d '\n')  && 00a_prepare_directory_dpm1.sh \"$VARIABLE\"",
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

    download = SSHOperator(
        task_id="01b_download.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 01b_download.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    symlink = SSHOperator(
        task_id="02a_symlink_data.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 02a_symlink_data.sh ""',
        cmd_timeout=None,
        conn_timeout=None
    )

    stackproc_runfile_setup = SSHOperator(
        task_id="03_create_run_script_nisar.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 03_create_run_script_nisar.sh ""',
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

    auto_control_run1 = SSHOperator(
        task_id="04_auto_control.sh_start_run1.sh",
        ssh_conn_id='ssh',
#        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; echo $PATH; which sbatch;',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run1" "start" "run1" "run1"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run2 = SSHOperator(
        task_id="04_auto_control.sh_start_run2.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run2" "start" "run2" "run2"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run3 = SSHOperator(
        task_id="04_auto_control.sh_start_run3.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run3" "start" "run3" "run3"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run4 = SSHOperator(
        task_id="04_auto_control.sh_start_run4.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run4" "start" "run4" "run4"',
        cmd_timeout=None,
        conn_timeout=None
    )

    auto_control_run5 = SSHOperator(
        task_id="04_auto_control.sh_start_run5.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run5" "start" "run5" "run5"',
        cmd_timeout=None,
        conn_timeout=None
    )

    # create_dpm2_runfiles = SSHOperator(
    #     task_id="05b_create_dpm2_runfiles",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 05b_create_dpm2_runfiles.sh ""',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_run_dpm2_1 = SSHOperator(
    #     task_id="06_run_dpm2_1",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_1" "start" "run_dpm2_1" "run_dpm2_1"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_run_dpm2_2 = SSHOperator(
    #     task_id="06_run_dpm2_2",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_2" "start" "run_dpm2_2" "run_dpm2_2"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_run_dpm2_3 = SSHOperator(
    #     task_id="06_run_dpm2_3",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_3" "start" "run_dpm2_3" "run_dpm2_3"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # # conditional_continuation = PythonOperator(
    # #     task_id='continue_run_if_one_success',
    # #     python_callable=run_if_one_success,
    # #     provide_context=True,
    # #     trigger_rule=TriggerRule.ALL_DONE)

    # auto_control_run_dpm2_4 = SSHOperator(
    #     task_id="06_run_dpm2_4",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_4" "start" "run_dpm2_4" "run_dpm2_4"',
    #     cmd_timeout=None,
    #     conn_timeout=None,
    #     trigger_rule=TriggerRule.ONE_SUCCESS,

    # )
    # auto_control_run_dpm2_5 = SSHOperator(
    #     task_id="06_run_dpm2_5",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; rm -rf dpm2/probGV; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_5" "start" "run_dpm2_5" "run_dpm2_5"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_run_dpm2_6 = SSHOperator(
    #     task_id="06_run_dpm2_6",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_6" "start" "run_dpm2_6" "run_dpm2_6"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_run_dpm2_7 = SSHOperator(
    #     task_id="06_run_dpm2_7",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_7" "start" "run_dpm2_7" "run_dpm2_7"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_run_dpm2_8 = SSHOperator(
    #     task_id="06_run_dpm2_8",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_run_dpm2_8" "start" "run_dpm2_8" "run_dpm2_8"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    upload_greyscale = SSHOperator(
        task_id="08_upload_greyscale.sh",
        ssh_conn_id='ssh',
        command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 08_upload_greyscale.sh "{{ var.json[run_id].dir_name }}" ',
        cmd_timeout=None,
        conn_timeout=None
    )

    # ### UPDATE METHODS ###
    # choose_postevent_branch_operator = BranchPythonOperator(
    #     task_id='check_postevent_branch',
    #     python_callable=choose_postevent_branch
    # )


    # wait_for_postevent = PythonSensor(
    #     task_id='wait_for_postevent',
    #     python_callable=check_postevent_ready,
    #     mode='reschedule',
    #     poke_interval=30
    # )

    # update_selection_file = SSHOperator(
    #     task_id="update_selection_file",
    #     ssh_conn_id='ssh',
    #     command='''source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; echo '{{ ti.xcom_pull(key="post_event_selection_str") }}' >> selection_download.txt''',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # download_update = SSHOperator(
    #     task_id="01b_download_update.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 01b_download.sh ""',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # symlink_update = SSHOperator(
    #     task_id="02a_symlink_data_update.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 02a_symlink_data.sh ""',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # stackproc_runfile_setup_update = SSHOperator(
    #     task_id="03_create_run_script_xpm2_update.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 03_create_run_script_xpm2.sh ""',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_runu1 = SSHOperator(
    #     task_id="04_auto_control.sh_start_runu1.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_runu1" "start" "runu1" "runu1"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_runu1x5 = SSHOperator(
    #     task_id="04_auto_control.sh_start_runu1x5.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_runu1" "start" "runu1x5" "runu1x5"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_runu2 = SSHOperator(
    #     task_id="04_auto_control.sh_start_runu2.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_runu1" "start" "runu2" "runu2"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_runu3 = SSHOperator(
    #     task_id="04_auto_control.sh_start_runu3.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_runu1" "start" "runu3" "runu3"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_runu4 = SSHOperator(
    #     task_id="04_auto_control.sh_start_runu4.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_runu1" "start" "runu4" "runu4"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_runu5 = SSHOperator(
    #     task_id="04_auto_control.sh_start_runu5.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_runu1" "start" "runu5" "runu5"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # auto_control_runu6 = SSHOperator(
    #     task_id="04_auto_control.sh_start_runu6.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 04_auto_control.sh "{{ var.json[run_id].dir_name }}_runu1" "start" "runu6" "runu6"',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )

    # generate_slcstk2cor_update = SSHOperator(
    #     task_id="05a_generate_slcstk2cor_update.sh",
    #     ssh_conn_id='ssh',
    #     command='source ~/.bash_profile; cd urgent_response/{{ var.json[run_id].dir_name }}; 05a_generate_slcstk2cor.sh ""',
    #     cmd_timeout=None,
    #     conn_timeout=None
    # )


    send_slack = SlackWebhookOperator(
        task_id='send_slack_notifications',
        slack_webhook_conn_id='slack_webhook_dpm2',
        message=':blob_excited:On your MacBook, run the following scripts to download DPM2 products:blob_excited:\n```\nscp -r aws-hpc2:/home/ubuntu/urgent_response/{{ var.json[run_id].dir_name }}/dpm2/probGV/\*tif .\n```\n \n',
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

    set_variable_task >> prepare_directory >> [get_dem, update_download_config]
    update_download_config >> download >> symlink
    [get_dem, symlink]>> stackproc_runfile_setup >> \
    auto_control_run1 >> auto_control_run2 >> auto_control_run3 >> auto_control_run4 >> auto_control_run5 >> \
    send_slack >> upload_greyscale >> update_job_status >> cleanup_task


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
