from kubernetes import client, config
import pandas as pd
import subprocess
import time
import os
import re

config.load_kube_config()
kube_api = client.CoreV1Api()


def get_openfaas_replica_ready_time(node_name, fun_name):
    pods = kube_api.list_namespaced_pod(
        namespace="EINS", field_selector=f"spec.nodeName={node_name}"
    )
    replica_ready_time = {}
    replica_scheduled_time = {}
    replica_start_time = {}
    for pod in pods.items:
        if pod.metadata.labels and pod.metadata.labels.get("faas_function") == fun_name:
            pod_name = pod.metadata.name
            replica_start_time[pod_name] = pod.status.start_time
            pod_conditions = pod.status.conditions
            for condition in pod_conditions:
                if condition.type == "PodScheduled" and condition.status == "True":
                    scheduled_time = condition.last_transition_time
                    replica_scheduled_time[pod_name] = scheduled_time
                if condition.type == "Ready" and condition.status == "True":
                    ready_time = condition.last_transition_time
                    if ready_time is None:
                        continue
                    replica_ready_time[pod_name] = ready_time
    return replica_start_time, replica_scheduled_time, replica_ready_time


def get_time_pd_data(node_name, fun_name, replica_num):
    while True:
        start_times, scheduled_times, ready_times = get_openfaas_replica_ready_time(
            node_name, fun_name
        )
        if len(ready_times) >= replica_num:
            break
        else:
            time.sleep(1)
    time.sleep(2 * replica_num)
    data = []
    for pod_name in ready_times.keys():
        print(
            f"Pod:{pod_name} start_times: {start_times[pod_name]}  Scheduled Time: {scheduled_times[pod_name]} Ready Time: {ready_times[pod_name]} Time Taken:{(ready_times[pod_name] - scheduled_times[pod_name]).total_seconds()}"
        )
        pod_data = {
            "function_name": FUNCTION_NAME,
            "Pod": pod_name,
            "muti_num": replica_num,
            "start_times": start_times[pod_name].timestamp(),
            "ScheduledTime": scheduled_times[pod_name].timestamp(),
            "ReadyTime": ready_times[pod_name].timestamp(),
            "TimeTaken": (
                ready_times[pod_name] - scheduled_times[pod_name]
            ).total_seconds(),
        }
        data.append(pod_data)
    df = pd.DataFrame(data)
    df.to_csv(
        f"../motivation/result/pull_time_{node_name}_{fun_name}.csv",
        mode="a",
        header=False,
        index=False,
    )
    return data


def convert_to_mb(size_str):
    unit_scaling = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }
    size_str = str(
        size_str
    ).upper()  # Convert to uppercase to handle case-insensitive units
    match = re.match(r"^(\d+)\s*([A-Z]+)?$", size_str)
    if match:
        size_value, size_unit = match.groups()
        if size_unit is None:
            size_unit = "B"
        if size_unit in unit_scaling:
            size_in_mb = int(size_value) * unit_scaling[size_unit] / unit_scaling["MB"]
            return size_in_mb
    raise ValueError(f"Invalid storage size string: {size_str}")


def get_function_pull_size(funtion):
    df = pd.read_csv("../profile/pull_size.csv")
    filtered_df = df[(df["model_name"] == funtion)]
    if len(filtered_df) == 0:
        return None
    else:
        return convert_to_mb(filtered_df.iloc[0]["pull_size"])


def deploy_function(replicas):
    pods = kube_api.list_namespaced_pod(
        namespace="EINS", field_selector=f"spec.nodeName={NODE_NAME}"
    )
    if len(pods.items) != 0:
        delete_function()
    command = f"faas-cli deploy -f {YAML_PATH} --filter {FUNCTION_NAME} --namespace=EINS --label com.openfaas.scale.min={replicas} --label com.openfaas.scale.max={replicas} --gateway {OPENFAAS_GATEWAY}"
    subprocess.run(command, shell=True)
    print(f"deploy {replicas} {FUNCTION_NAME}")
    time.sleep(2 * replicas)


def delete_function():
    # Delete the deployed function
    time.sleep(5)
    command = f"faas-cli remove {FUNCTION_NAME} --gateway={OPENFAAS_GATEWAY} --namespace=EINS"
    subprocess.run(command, shell=True)
    print(f"delete {FUNCTION_NAME}")
    time.sleep(10)
    while True:
        pods = kube_api.list_namespaced_pod(
            namespace="EINS", field_selector=f"spec.nodeName={NODE_NAME}"
        )
        if len(pods.items) == 0:
            break
        else:
            time.sleep(3)
    time.sleep(10)


OPENFAAS_GATEWAY = "http://gateway_url:port/"  # Adjust to your OpenFaaS gateway URL
global FUNCTION_NAME
FUNCTION_NAME = "shufflenet"
NODE_NAME = ""
YAML_PATH = "openfass-deep-learning.yml"
BASE_DIR = "../benchmark/"


if __name__ == "__main__":
    os.chdir(BASE_DIR)
    print("Started")
    functions = [
        "mnist",
        "mobilenet",
        "resnet18",
        "resnet34",
        "resnet50",
        "vggnet11",
    ]
    for fun in functions:
        FUNCTION_NAME = fun
        CSV_FILE = f"../motivation/result/pull_time_result_{FUNCTION_NAME}.csv"
        function_pull_size = get_function_pull_size(FUNCTION_NAME)
        delete_function()
        for replica_num in range(1, 31):
            deploy_function(replica_num)
            time.sleep(replica_num)
            data = get_time_pd_data(NODE_NAME, FUNCTION_NAME, replica_num)
            if len(data) != replica_num:
                print("Failed to get ready time")
                continue
            total_time_taken = sum(pod_data["TimeTaken"] for pod_data in data)
            avg_time = total_time_taken / len(data)
            results_df = pd.DataFrame(
                {
                    "function_name": [FUNCTION_NAME],
                    "pull_size": [function_pull_size],
                    "replica_num": [replica_num],
                    "avg_time": [avg_time],
                }
            )
            results_df.to_csv(CSV_FILE, mode="a", header=False, index=False)
            delete_function()
        print(f"{FUNCTION_NAME} results written to {CSV_FILE}")
