import time, os, logging
import asyncio
import subprocess
import atexit, nest_asyncio
from datetime import datetime
import matplotlib.pyplot as plt
from cachetools import TTLCache, cached
from kubernetes import client, config

nest_asyncio.apply()
current_path = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_path)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S ",
    filename="evaluate.log",
)


# @cached(TTLCache(maxsize=1024, ttl=60))
def build_url_list(wrknames):
    url_list = {}
    for model in wrknames:
        url_list[model]=f"http://gateway_url:port/function/{model}.{NAMESPACE}"
    return url_list


async def invoke_fn(url, requests, concurrent):
    requests = int(requests)
    # qps = int(requests/concurrent)
    model_name = url.split("/")[-1].split("-")[0]
    timestamp = time.time().__int__()
    if in_time:
        outputfile = f"{output_directory}{model_name}_c{concurrent}_z{duration_time}_{timestamp}.csv"
        # -cpus 1
        request_hey = f"hey -o csv -z {duration_time} -c {concurrent} -t 0 {url}> {outputfile}"
        print(f"{request_hey}")
        logging.info(f"requesting {duration_time} of {url}")
    else:
        outputfile = (
            f"{output_directory}{model_name}_r{requests}_c{concurrent}_{timestamp}.csv"
        )
        # -cpus 1
        request_hey = (
            f"hey -o csv -n {requests} -c {concurrent} -t 0 {url}> {outputfile}"
        )
        print(f"{request_hey}")
        # print(f"requesting {requests} of {url}")
        logging.info(f"requesting {requests} of {url}")
    hey_porcess = await asyncio.create_subprocess_shell(request_hey)
    hey_processes.append(hey_porcess)


async def send_request(requests, concurrent):
    logging.info(
        f"Test with {requests} requests and {concurrent} concurrent threads completed."
    )
    requests_vs_url = {}
    url_list = build_url_list(functions)
    for url in list(url_list.keys()):
        # requests_vs_url[url] = int(requests / len(functions))
        requests_vs_url[url] = requests
    # call invoke_fn for each url at the same time
    start_time = datetime.now()
    tasks = []
    url = url_list[FUNCTION_NAME]
    tasks.append(invoke_fn(url, requests_vs_url[FUNCTION_NAME], concurrent))
    await asyncio.gather(*tasks)
    end_time = datetime.now()
    exec_time = (end_time - start_time).total_seconds()
    # print(f"Execution time: {exec_time} seconds")
    logging.info(f"Execution time: {exec_time} seconds")


def count_hey_processes():
    process = subprocess.Popen(["ps", "-ef"], stdout=subprocess.PIPE)
    grep = subprocess.Popen(
        ["grep", "hey -o csv"], stdin=process.stdout, stdout=subprocess.PIPE
    )
    process.stdout.close()  # Allow process to receive a SIGPIPE if grep exits.
    output = subprocess.check_output(["wc", "-l"], stdin=grep.stdout)
    grep.stdout.close()  # Allow wc to receive a SIGPIPE if grep exits.
    return int(output)

def deploy_function(replicas):
    pods = kube_api.list_namespaced_pod(namespace=NAMESPACE)
    if len(pods.items) != 0:
        delete_function()
    os.chdir(YAML_DIR)
    command = f"faas-cli deploy -f {YAML_PATH} --filter {FUNCTION_NAME} --namespace={NAMESPACE} --gateway {OPENFAAS_GATEWAY}"
    subprocess.run(command, shell=True)
    print(f"deploy {replicas} {FUNCTION_NAME}")
    os.chdir(current_path)
    time.sleep(replicas)


def delete_function():
    # Delete the deployed function
    time.sleep(5)
    command = f"faas-cli remove {FUNCTION_NAME} --gateway={OPENFAAS_GATEWAY} --namespace={NAMESPACE}"
    subprocess.run(command, shell=True)
    print(f"delete {FUNCTION_NAME}")
    time.sleep(10)
    while True:
        pods = kube_api.list_namespaced_pod(namespace=NAMESPACE)
        if len(pods.items) == 0:
            break
        else:
            time.sleep(3)
    time.sleep(10)


def cleanup():
    os.system("pkill -9 hey")
    print("Clean up")
    logging.info("Clean up")


atexit.register(cleanup)


def create_hpa():
    cmd_create = f"kubectl autoscale deployment {FUNCTION_NAME} -n {NAMESPACE} --cpu-percent=30  --min=1  --max=30"
    cmd_watch = f"kubectl get hpa/{FUNCTION_NAME} -n {NAMESPACE}"
    # action_names=['cnn','mnist','lstm','vggnet11','mobilenet','shufflenet','resnet18','resnet34','resnet50']
    os.system(cmd_create)
    os.system(cmd_watch)


def delete_hpa():
    cmd_deleteHPA = f"kubectl delete hpa {FUNCTION_NAME} -n {NAMESPACE}"    
    cmd_watch = f"kubectl get hpa/{FUNCTION_NAME} -n {NAMESPACE}"    
    os.system(cmd_deleteHPA)
    os.system(cmd_watch)

# atexit.register(delete_hpa)


# Test different functions, request volumes, and thread counts
functions = [
    "mnist", 
    "vggnet11",
    "mobilenet",
    "resnet18",
    "resnet34",
    "resnet50",
]
global FUNCTION_NAME
FUNCTION_NAME = functions[0]
config.load_kube_config()
kube_api = client.CoreV1Api()
thread_counts = [20,]
request_volumes = [1000 * len(functions)]
in_time = True
duration_time = "1800s"
# Duration of application to send requests.If duration is specified, n is ignored.Examples: -z 10s -z 3m.
base_url = "http://gateway_url:port/function/"
scheduler = "MArk"
# scheduler = "Tetris"
# scheduler = "EINS"
NAMESPACE = "EINS"

hey_processes = []
OPENFAAS_GATEWAY = "http://gateway_url:port/"  # Adjust to your OpenFaaS gateway URL
YAML_PATH = "openfass-deep-learning.yml"
YAML_DIR = "../benchmark/"

async def main():
    for reqs in request_volumes:
        deploy_function(1)
        if scheduler == "defalt":
            create_hpa()
        for concurrent in thread_counts:
            start_time = datetime.now()
            await send_request(reqs, concurrent)
            end_time = datetime.now()
            total_time = (end_time - start_time).total_seconds()
            print(f"Total time: {total_time} seconds")
            logging.info(f"Total time: {total_time} seconds")

if __name__ == "__main__":
    for fun in functions:
        # try:
            FUNCTION_NAME = fun
            output_directory = f"hey_results/{scheduler}/{FUNCTION_NAME}/"
            logging.info(f"evaluating with {scheduler} scheduler")
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)
            print("Started")
            cleanup()
            # main()
            # # # atexit.register(cleanup)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main())
            alldone = False
            while not alldone:
                if count_hey_processes() == 1:
                    alldone = True
                logging.info(f"Waiting for {count_hey_processes()} hey processes to finish")
                time.sleep(10)
            delete_function()
            cleanup()
        # except:
        #     pass
        #     continue
    exit(0)
