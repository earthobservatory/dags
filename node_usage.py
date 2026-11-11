"""
node_usage: the cluster's compute nodes, as ParallelCluster started and
stopped them, for SARimpact's per-job cost (Pipeline page, Jobs tab).

SARimpact triggers this DAG every hour; nothing schedules it here. conf:

  {"since": <unix seconds>}
      Every node run that was still up at or after `since` (0: all of them).

ParallelCluster logs each node it starts (slurm_resume.log: the Slurm node
name and its EC2 instance id) and each node Slurm powers down after
SuspendTime idle (slurm_suspend.log). Paired up, they give every node's
up time; with the queue's instance type from the cluster config, its cost.
All queues are on-demand and scale to zero, so that is the whole compute
bill bar the head node (checked against Cost Explorer, Nov 2026: within 3%).

The SSH task prints JSON between NODE_USAGE_BEGIN and NODE_USAGE_END:

  {"cluster": "eos-aws3", "now": 1762850000, "head": "m6a.2xlarge",
   "queues": {"queue-2": {"types": ["r7a.4xlarge"], "capacity": "ONDEMAND"}},
   "runs": [["queue-2-dy-queue-2-cr-0-6", "i-0adf...", 1762761100, 1762764424, ""]]}

Each run is [node, instance id, launched, stopped or null if still up, how
it ended]: "" when Slurm suspended it, "relaunch" when the node name came up
again with no suspend logged in between (it is closed at the relaunch).

The parser runs on the head node from the base64 below, so nothing from conf
reaches the shell but a checked integer.

A second task, read_tasks, returns (as JSON, its XCom) every SSH task that
ran since `since` in any DAG but this one and vault (those run on the head
node, not on compute nodes): SARimpact splits each node's time over the
tasks running while it was up, so it needs runs started outside SARimpact
too, or their nodes would be charged to SARimpact's jobs.

  {"tasks": [["S1_DPM3", "manual__2026-10-16T10:32:26+00:00", "05_stack.sh", -1,
              1760610000.0, 1760620000.0, "success"]]}

Each is [dag id, run id, task id, map index, started, ended or null, state];
Airflow keeps one row per task, so a retried task shows its last try only.

It also returns what each of those runs was, so SARimpact can name the runs
it did not start and count them toward an organization:

  {"runs": [{"dag_id": "S1_FPM3", "run_id": "manual__...", "state": "success",
             "logical_date": 1760610000.0, "started_by": "ricky",
             "config": {"organization": "NSAS", "event_name": "...", "dir_name": "..."}}]}

`config` holds only CONFIG_KEYS, read from the run's payload: the seed task's
`run_payload` XCom (kept after cleanup_variables deletes the Variable), else
the orch seed task's return value, else the run's Variable, else its trigger
conf. Put "organization" (an organization's short name, name or id in
SARimpact) in a run's variables and its cost counts toward that organization.
`started_by` is the user Airflow's audit log has triggering the DAG then
(best effort; empty when none matches).
"""

import base64
import json
import shlex
from datetime import datetime, timezone

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

PARSER = r'''
import glob, json, re, subprocess, sys, time
from datetime import datetime, timezone

LOGS = "/var/log/parallelcluster/"
CONFIG = "/opt/parallelcluster/shared/cluster-config.yaml"
since = int(sys.argv[1]) if len(sys.argv) > 1 else 0

def stamp(line):
    return int(datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())

def lines(name):
    # A rotated copy (name.1) first, then the live log; gzipped ones are skipped.
    for path in sorted(glob.glob(LOGS + name + ".[0-9]"), reverse=True) + [LOGS + name]:
        try:
            with open(path, errors="replace") as handle:
                yield from handle
        except OSError:
            pass

expanded = {}
def hostnames(expr):
    if "[" not in expr:
        return [expr]
    if expr not in expanded:
        out = subprocess.run(["scontrol", "show", "hostnames", expr], capture_output=True, text=True).stdout.split()
        expanded[expr] = out or [expr]
    return expanded[expr]

LAUNCH = re.compile(r"Nodes are now configured with instances \(x\d+\) \[(.*)\]")
PAIR = re.compile(r"\('([^']+)', EC2Instance\(id='([^']+)'")
SUSPEND = re.compile(r"Suspending following nodes\.[^:]*: (.+)$")

events = []
for line in lines("slurm_resume.log"):
    m = LAUNCH.search(line)
    if m:
        t = stamp(line)
        for node, instance in PAIR.findall(m.group(1)):
            events.append((t, 0, node, instance))
for line in lines("slurm_suspend.log"):
    m = SUSPEND.search(line)
    if m:
        t = stamp(line)
        for part in re.split(r",(?![^\[]*\])", m.group(1).strip()):
            for node in hostnames(part.strip()):
                events.append((t, 1, node, ""))
events.sort()

runs, up = [], {}
for t, kind, node, instance in events:
    if kind == 0:
        if node in up:
            runs.append(up.pop(node) + [t, "relaunch"])
        up[node] = [node, instance, t]
    elif node in up:
        runs.append(up.pop(node) + [t, ""])
runs.extend(run + [None, ""] for run in up.values())
runs = [r for r in runs if r[3] is None or r[3] >= since]
runs.sort(key=lambda r: r[2])

queues, head = {}, ""
try:
    import yaml
    config = yaml.safe_load(open(CONFIG))
    head = config["HeadNode"]["InstanceType"]
    for queue in config["Scheduling"]["SlurmQueues"]:
        types = []
        for resource in queue.get("ComputeResources", []):
            if resource.get("InstanceType"):
                types.append(resource["InstanceType"])
            types += [i["InstanceType"] for i in resource.get("Instances", [])]
        queues[queue["Name"]] = {"types": types, "capacity": queue.get("CapacityType", "ONDEMAND")}
except Exception as exc:
    print("cluster config unreadable:", exc, file=sys.stderr)

cluster = ""
try:
    cluster = re.search(r"ClusterName\s*=\s*(\S+)", subprocess.run(
        ["scontrol", "show", "config"], capture_output=True, text=True).stdout).group(1)
except Exception:
    pass

print("NODE_USAGE_BEGIN")
print(json.dumps({"cluster": cluster, "now": int(time.time()), "head": head, "queues": queues, "runs": runs},
                 separators=(",", ":")))
print("NODE_USAGE_END")
'''

PARSER_B64 = base64.b64encode(PARSER.encode()).decode()


# The operators that run work on the cluster; and DAGs whose SSH tasks only
# touch the head node.
CLUSTER_OPERATORS = ("SSHOperator", "OrchSSHOperator")
HEAD_NODE_DAGS = ("node_usage", "vault")

# The run payload: where the seed tasks keep it, and the keys SARimpact reads.
SEED_TASKS = ("set_variables", "set_and_store_variables")
CONFIG_KEYS = ("organization", "event_name", "dir_name")
TRIGGER_WINDOW = 120  # seconds either side of a run's logical date


def _since(context) -> int:
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    since = conf.get("since", 0)
    if isinstance(since, bool) or not isinstance(since, int) or since < 0:
        raise AirflowFailException("since must be a non-negative integer (unix seconds)")
    return since


def build_command(**context):
    return f"echo {PARSER_B64} | base64 -d | python3 - {shlex.quote(str(_since(context)))}"


def read_tasks(**context):
    from sqlalchemy import or_

    from airflow.models import TaskInstance
    from airflow.utils.session import create_session

    start = datetime.fromtimestamp(_since(context), tz=timezone.utc)
    stamp = lambda when: when.timestamp() if when else None  # noqa: E731
    with create_session() as session:
        rows = (
            session.query(
                TaskInstance.dag_id, TaskInstance.run_id, TaskInstance.task_id, TaskInstance.map_index,
                TaskInstance.start_date, TaskInstance.end_date, TaskInstance.state,
            )
            .filter(TaskInstance.operator.in_(CLUSTER_OPERATORS))
            .filter(TaskInstance.dag_id.notin_(HEAD_NODE_DAGS))
            .filter(TaskInstance.start_date.isnot(None))
            .filter(or_(TaskInstance.end_date >= start, TaskInstance.end_date.is_(None)))
            .all()
        )
        tasks = [[r.dag_id, r.run_id, r.task_id, r.map_index, stamp(r.start_date), stamp(r.end_date), r.state]
                 for r in rows]
        runs = [_describe_run(session, dag_id, run_id, stamp) for dag_id, run_id in sorted({(r[0], r[1]) for r in tasks})]
    return json.dumps({"tasks": tasks, "runs": runs}, separators=(",", ":"))


def _as_config(value):
    """A payload (dict, or a JSON string of one) cut down to CONFIG_KEYS; None if it is not one."""
    if isinstance(value, (str, bytes)):
        try:
            value = json.loads(value)
        except ValueError:
            return None
    if not isinstance(value, dict):
        return None
    return {key: value[key] for key in CONFIG_KEYS if value.get(key) not in (None, "")}


def _describe_run(session, dag_id, run_id, stamp):
    from datetime import timedelta

    from airflow.models import DagRun, Log, Variable, XCom

    dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id).one_or_none()
    config = None
    for key in ("run_payload", "return_value"):
        for task_id in SEED_TASKS:
            if config is None:
                try:
                    config = _as_config(XCom.get_one(
                        key=key, dag_id=dag_id, task_id=task_id, run_id=run_id, session=session))
                except Exception:  # noqa: BLE001 - an unreadable XCom is no payload
                    config = None
    if config is None:
        config = _as_config(Variable.get(run_id, default_var=None))
    if config is None and dag_run is not None:
        config = _as_config(dag_run.conf)

    started_by = ""
    logical = dag_run.execution_date if dag_run is not None else None
    if logical is not None:
        window = timedelta(seconds=TRIGGER_WINDOW)
        entry = (
            session.query(Log.owner)
            .filter(Log.dag_id == dag_id, Log.dttm >= logical - window, Log.dttm <= logical + window)
            .filter(Log.event.ilike("%trigger%") | Log.event.ilike("%post_dag_run%"))
            .order_by(Log.dttm)
            .first()
        )
        started_by = (entry.owner or "") if entry else ""
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "state": dag_run.state if dag_run is not None else "",
        "logical_date": stamp(logical),
        "started_by": started_by,
        "config": config or {},
    }


with DAG(
    dag_id="node_usage",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sarimpact", "cost"],
    doc_md=__doc__,
) as dag:
    command = PythonOperator(
        task_id="build_command",
        python_callable=build_command,
        doc_md="Check `since` and build the command",
    )

    read_nodes = SSHOperator(
        task_id="read_nodes",
        ssh_conn_id="ssh",
        command="{{ ti.xcom_pull(task_ids='build_command') }}",
        cmd_timeout=600,
        conn_timeout=60,
        doc_md="Read the node logs on aws-hpc-c3",
    )

    tasks = PythonOperator(
        task_id="read_tasks",
        python_callable=read_tasks,
        doc_md="List the SSH tasks of every DAG run since `since`, from Airflow's database",
    )

    command >> read_nodes
