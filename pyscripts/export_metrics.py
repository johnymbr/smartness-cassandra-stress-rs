import requests
import csv
from datetime import datetime

proxies = {
    'http': 'socks5://localhost:1337',
    'https': 'socks5://localhost:1337'
}

# METRICS = [
#     "node_cpu_seconds_total",
#     "node_memory_MemAvailable_bytes",
#     "node_network_receive_bytes_total",
#     "node_disk_io_time_seconds_total"
# ]

STEP = "1s"
START = 1762727858
END = 1762735660


def run():
    global START, END, STEP, HOURS

    # Dicionário para armazenar os valores: {timestamp: {metric1: val, metric2: val...}}
    data = {}

    processed_metrics = ["node_cpu_usage", "user_cpu_usage", "system_cpu_usage", "memory_usage_per_container",
                         "network_receive_bytes_per_container", "network_transmit_bytes_per_container"]

    # retrieve all metrics
    all_metrics_resp = requests.get("http://prometheus.cassandracluster.com/api/v1/label/__name__/values",
                                    proxies=proxies)
    metrics = all_metrics_resp.json()["data"]
    metrics = metrics + processed_metrics
    # metrics.append("node_cpu_usage")
    # metrics.append("user_cpu_usage")
    # metrics.append("system_cpu_usage")
    # metrics.append("memory_usage_per_container")
    # metrics.append("network_receive_bytes_per_container")
    # metrics.append("network_transmit_bytes_per_container")
    metrics_aliases = []
    count = 0

    query_end = START + 300
    if query_end > END:
        query_end = END

    while query_end <= END:
        print(f"Search START: {START} - END: {query_end}")
        for metric in metrics:
            if not metric.startswith("container_") and not metric.startswith(
                    "node_") and metric not in processed_metrics:
                continue

            query = metric
            if metric.startswith("container_"):
                query += '{container_label_com_docker_stack_namespace!=""}'
            elif metric.startswith("node_"):
                query += '{mode="user"}'
            elif metric == "node_cpu_usage":
                query = "(sum(irate(container_cpu_usage_seconds_total{container_label_com_docker_stack_namespace!=\"\"}[5s])) by (instance)) * 100"
            elif metric == "user_cpu_usage":
                query = "sum(rate(container_cpu_user_seconds_total{container_label_com_docker_stack_namespace!=\"\"}[5s])) by (name) * 100"
            elif metric == "system_cpu_usage":
                query = "sum(rate(container_cpu_system_seconds_total{container_label_com_docker_stack_namespace!=\"\"}[5s])) by (name) * 100"
            elif metric == "memory_usage_per_container":
                query = "(container_memory_working_set_bytes{container_label_com_docker_stack_namespace!=\"\"} / ignoring(container_spec_memory_limit_bytes) (container_spec_memory_limit_bytes{container_label_com_docker_stack_namespace!=\"\"} > 0)) * 100"
            elif metric == "network_receive_bytes_per_container":
                query = "rate(container_network_receive_bytes_total{container_label_com_docker_stack_namespace!=\"\"}[5s])"
            elif metric == "network_transmit_bytes_per_container":
                query = "rate(container_network_transmit_bytes_total{container_label_com_docker_stack_namespace!=\"\"}[5s])"

            count += 1
            r = requests.get(
                "http://prometheus.cassandracluster.com/api/v1/query_range",
                params={
                    "query": query,
                    "start": START,
                    "end": query_end,
                    "step": STEP
                },
                proxies=proxies
            )
            results = r.json()["data"]["result"]

            if not results:
                continue

            # Seleciona apenas a primeira série retornada
            for index, metric_data in enumerate(results):
                metric_alias = f"{metric}_{index}"
                if not metric_alias in metrics_aliases:
                    metrics_aliases.append(metric_alias)

                for timestamp, value in metric_data["values"]:
                    ts = int(timestamp)
                    if ts not in data:
                        data[ts] = {}
                    data[ts][metric_alias] = value

        START = query_end
        query_end = START + 300
        if START < END < query_end:
            query_end = END

    print(f"Found {len(metrics_aliases)} metric aliases")

    # Ordenar por timestamp e salvar no CSV
    with open("prometheus_metrics_wide.csv", "w", newline="") as csvfile:
        fieldnames = ["timestamp"] + list(metrics_aliases)
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for ts in sorted(data.keys()):
            row = {"timestamp": ts}
            row.update(data[ts])
            writer.writerow(row)


if __name__ == '__main__':
    run()
