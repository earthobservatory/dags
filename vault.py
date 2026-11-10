"""
vault: SARimpact's window on the cluster's run folders.

SARimpact (Pipeline page, Folders tab) triggers this DAG; nothing schedules
it. Three actions, picked by dag_run.conf:

  {"action": "report"}
      Runs `vault --report` on aws-hpc-c3: every folder in urgent_response and
      response_archive with its size, last change and flags, as JSON. The SSH
      task's output is its XCom, which SARimpact reads back over the REST API.

  {"action": "labels", "labels_b64": "<base64 JSON>"}
      Runs `vault --set-labels`, which saves SARimpact's label for each folder
      (flag, unflag, clear, keep, check) to ~/.vault/labels.json, where the
      `vault` picker shows them as a column.

  {"action": "apply", "apply_b64": "<base64 JSON>"}
      Runs `vault --apply`: sets or removes deep_storage_flag on the folders
      staff chose Flag or Unflag for on the Folders tab, as vault's own flag
      and remove-flag options do, and prints each folder's outcome.

None of them moves or deletes a folder: moving stays with a person in
`vault`, and deleting with wildfly-utils' deep_archive.sh.

The command is built in a Python task from validated conf (the action from a
fixed list, the labels and folder lists as plain base64), so nothing from conf reaches the
shell unquoted.
"""

import re
import shlex
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

VAULT = "/home/ubuntu/insarscripts/stack_processor_aws/utils/archive_picker.sh"
BASE64 = re.compile(r"^[A-Za-z0-9+/=]+$")
MAX_LABELS_B64 = 2 * 1024 * 1024


def build_command(**context):
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    action = conf.get("action", "report")
    if action == "report":
        return f"bash {shlex.quote(VAULT)} --report"
    if action == "labels":
        labels = conf.get("labels_b64") or ""
        if not BASE64.match(labels) or len(labels) > MAX_LABELS_B64:
            raise AirflowFailException("labels_b64 must be plain base64 under 2 MB")
        return f"bash {shlex.quote(VAULT)} --set-labels {shlex.quote(labels)}"
    if action == "apply":
        request = conf.get("apply_b64") or ""
        if not BASE64.match(request) or len(request) > MAX_LABELS_B64:
            raise AirflowFailException("apply_b64 must be plain base64 under 2 MB")
        return f"bash {shlex.quote(VAULT)} --apply {shlex.quote(request)}"
    raise AirflowFailException(f"unknown action {action!r}; expected report, labels or apply")


with DAG(
    dag_id="vault",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sarimpact", "storage"],
    doc_md=__doc__,
) as dag:
    command = PythonOperator(
        task_id="build_command",
        python_callable=build_command,
        doc_md="Check the request and build the vault command",
    )

    run_vault = SSHOperator(
        task_id="run_vault",
        ssh_conn_id="ssh",
        command="{{ ti.xcom_pull(task_ids='build_command') }}",
        cmd_timeout=1800,
        conn_timeout=60,
        doc_md="Run vault on aws-hpc-c3",
    )

    command >> run_vault
