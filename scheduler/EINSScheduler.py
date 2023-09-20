# from gevent import monkey
# from gevent.pywsgi import WSGIServer
# monkey.patch_all()
import re
import threading
import time
import random
import json
import ssl, os
import base64
import logging
import pandas as pd
from flask import Flask, request

from kubernetes import client, config
from prometheus_api_client import PrometheusConnect
from redisClient import RedisClient

current_path = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_path)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S ",
    filename="EINSSchedule.log",
)
redis_client = RedisClient("redis_url", port)

os.chdir(os.path.dirname(os.path.abspath(__file__)))
context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
context.load_cert_chain(
    "mutaingwebhook/server.crt", "mutaingwebhook/server.key"
)
config.load_kube_config()
api = client.CoreV1Api()
v1 = client.AppsV1Api()

prom = PrometheusConnect(url="http://Prometheus_url")

scheduler_name = "EINSScheduler"
namespace = "EINS"

unit_scaling = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}

def convert_to_mb(size_str):
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


def patch_pod(pod, node_name):
    # Patch the pod to the selected node and device
    patch_operation = [
        # pod name
        {
            "op": "add",
            "path": "/spec/nodeSelector",
            "value": {"kubernetes.io/hostname": node_name},
        },
        {
            "op": "add",
            "path": "/spec/containers/0/readinessProbe",
            "value": {
                "httpGet": {"path": "/loaded", "port": 5000},
                "initialDelaySeconds": 1,
                "periodSeconds": 1,
                "timeoutSeconds": 160,
            },
        },
    ]
    return base64.b64encode(json.dumps(patch_operation).encode("utf-8"))


def admission_response(uid, message, pod, node_name):
    if not node_name:
        return json.dumps(
            {
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "response": {
                    "uid": uid,
                    "allowed": False,
                    "status": {"message": message},
                },
            }
        )
    # Create an admission response
    return json.dumps(
        {
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": {
                "uid": uid,
                "allowed": True,
                "status": {"message": message},
                "patchType": "JSONPatch",
                "patch": patch_pod(pod, node_name).decode("utf-8"),
            },
        }
    )


THRESHOLD = 14000000


def check_pull_num(node):    
    query = f'count(irate(container_network_receive_bytes_total{{namespace="EINS", node="{node}"}}[1m]) > {THRESHOLD})'
    result = prom.custom_query(query=query)
    if result:
        return int(result[0]["value"][1])
    else:
        return 0


def get_pending_pods_count(namespace, node_name):
    pods = api.list_namespaced_pod(
        namespace, field_selector=f"spec.nodeName={node_name}", watch=False
    )
    pending_count = 0
    for pod in pods.items:
        if pod.status.phase == "Pending":
            pending_count += 1
    return pending_count


def get_unready_pods_count(namespace, node_name):
    pods = api.list_namespaced_pod(
        namespace, field_selector=f"spec.nodeName={node_name}", watch=False
    )
    unready_count = 0
    for pod in pods.items:
        pod_name = pod.metadata.name
        pod_conditions = pod.status.conditions
        for condition in pod_conditions:
            if condition.type == "Ready" and condition.status == "False":
                unready_count += 1
    return unready_count


data_lock = threading.Lock()
global server_data_lock
server_data_lock = False
server_data = {"nodes": {}, "last_check_time": time.time()}


def create_server_data():
    node_list = api.list_node().items
    unableNode = [ ]  
    for node in node_list:
        node_name = node.metadata.name
        if node_name in unableNode:
            continue
        server_data["nodes"][node_name] = {
            "pull_size": 0,
            "last_deploy_time": time.time(),
            "last_assign_time": time.time(),
            "bandwidth": "100MB",#/s
        }
    return server_data


def update_pull_size():
    current_time = time.time()
    time_interval = current_time - server_data["last_check_time"]
    for node_name, node_info in server_data["nodes"].items():
        pull_size = node_info["pull_size"]
        bandwidth = node_info["bandwidth"]
        pulled_size = time_interval * convert_to_mb(bandwidth)
        updated_pull_size = max(0, pull_size - pulled_size)
        server_data["nodes"][node_name]["pull_size"] = updated_pull_size
        logging.info(f"Updated {node_name}: pull_size = {updated_pull_size}")


def assign_replica(function_name, num_to_deploy, pull_size):
    for i in range(num_to_deploy):
        selected_node = min(
            server_data["nodes"],
            key=lambda x: (
                server_data["nodes"][x]["pull_size"],
                server_data["nodes"][x]["last_assign_time"],
            ),
        )
        server_data["nodes"][selected_node]["pull_size"] += pull_size
        if function_name not in server_data["nodes"][selected_node]:
            server_data["nodes"][selected_node][function_name] = 1
        else:
            server_data["nodes"][selected_node][function_name] += 1
        server_data["nodes"][selected_node]["last_assign_time"] = time.time()
        logging.info(f"Deployed {function_name} {i} to {selected_node}")


def assignment_function(current_time, functions_to_deploy):
    logging.info(f"Check at {current_time}:{functions_to_deploy}")
    updated_pull_size = False
    for function_info in functions_to_deploy:
        function_name = function_info["name"]
        add_time = function_info["add_time"]
        if server_data.get("last_check_time", 0) > add_time:
            logging.info(
                f"Task for {function_name} added at {add_time} has been skipped."
            )
            functions_to_deploy.remove(function_info)
            continue
        else:
            logging.info(f"Start process {function_info}")
        pull_size = function_info["pull_size"]
        num_to_deploy = function_info["num_to_deploy"]
        if not updated_pull_size:
            update_pull_size()
            updated_pull_size = True
        assign_replica(function_name, num_to_deploy, pull_size)
        functions_to_deploy.remove(function_info)
    server_data["last_check_time"] = current_time
    logging.info(functions_to_deploy)
    return functions_to_deploy


def assignment_check():
    create_server_data()
    check_interval = 1 
    while True:
        current_time = time.time()
        if current_time - server_data["last_check_time"] >= check_interval:
            try:
                # lock = redis_client.get_lock("scale_up")
                lock = True
                if lock:
                    functions_to_deploy = json.loads(redis_client.get_dict("scale_up"))
                    if len(functions_to_deploy) == 0:
                        time.sleep(0.5)
                        continue
                    functions_to_deploy_new = assignment_function(
                        current_time, functions_to_deploy.copy()
                    )
                    redis_client.set_dict("scale_up", json.dumps(functions_to_deploy_new))
            finally:
                redis_client.release_lock("scale_up")
        else:
            time.sleep(0.5)


app = Flask(__name__)
NodeMaxPullNum = 3
NodeMaxPendingNum = 4
NodeMaxUnreadyNum = 3
Interval = 2
retry_count = 4


@app.route("/mutate", methods=["POST"])
async def mutate():
    review = request.json
    pod = review["request"]["object"]
    uid = review["request"]["uid"]
    function_name = pod["metadata"]["labels"].get("faas_function")
    receive_time = time.time()
    logging.info(f"Receive request for {function_name} {uid} at {receive_time}")
    for i in range(retry_count):
        if server_data.get("last_check_time", 0) < receive_time:
            time.sleep(0.4)
        else:
            break
    selected_node = None
    min_last_deploy_time = time.time()
    global server_data_lock
    while server_data_lock == True:
        time.sleep(0.2)
    server_data_lock == True
    for node, info in server_data["nodes"].items():
        if (
            function_name in info
            and info[function_name] > 0
            and info["last_deploy_time"] < min_last_deploy_time
        ):
            selected_node = node
            min_last_deploy_time = info["last_deploy_time"]
    deploy_time = time.time()
    if selected_node != None:
        server_data["nodes"][selected_node][function_name] -= 1
        server_data["nodes"][selected_node]["last_deploy_time"] = deploy_time
        server_data_lock = False
        while (
            check_pull_num(selected_node) > NodeMaxPullNum
            or get_unready_pods_count("EINS", selected_node) > NodeMaxUnreadyNum
        ) and (time.time() - receive_time < 20):
            random.seed(int((time.time() % 1) * 10000000))
            time.sleep(random.randrange(10, 100) / 10) 
        logging.info(
            f"Select {selected_node} for {function_name} {uid} at {time.time()}"
        )
        addmission = admission_response(
            review["request"]["uid"], "success", pod, selected_node
        )
        return addmission
    else:
        server_data_lock = False
        for node, info in server_data["nodes"].items():
            if info["last_deploy_time"] < min_last_deploy_time:
                selected_node = node
                min_last_deploy_time = info["last_deploy_time"]
        server_data["nodes"][selected_node]["last_deploy_time"] = time.time()
        logging.info(
            f"Defalt select {selected_node} for {function_name} {uid} at {time.time()}"
        )
        return admission_response(
            review["request"]["uid"], "success", pod, selected_node
        )
        # return admission_response(review["request"]["uid"], "fail", pod, selected_node)


# Start the assignment thread
processing_thread = threading.Thread(target=assignment_check)
processing_thread.daemon = True
processing_thread.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9008, ssl_context=context, threaded=True)
