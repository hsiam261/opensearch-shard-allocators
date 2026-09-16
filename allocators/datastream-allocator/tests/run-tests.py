#!/usr/bin/env python3

import argparse
import atexit
import json
import math
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import urllib.error
from typing import Any

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
COMPOSE_FILE = os.path.join(SCRIPT_DIR, "docker-compose.yml")
OS_URL = "http://localhost:9200"

RED = "\033[0;31m"
GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
CYAN = "\033[0;36m"
NC = "\033[0m"

PASS_COUNT = 0
FAIL_COUNT = 0


def log(msg: str) -> None:
    print(f"{YELLOW}>>> {msg}{NC}")


def info(msg: str) -> None:
    print(f"{CYAN}    {msg}{NC}")


def pass_test(msg: str) -> None:
    global PASS_COUNT
    print(f"{GREEN}  PASS: {msg}{NC}")
    PASS_COUNT += 1


def fail_test(msg: str) -> None:
    global FAIL_COUNT
    print(f"{RED}  FAIL: {msg}{NC}")
    FAIL_COUNT += 1


def ceil_div(a: int, b: int) -> int:
    return math.ceil(a / b)


def os_request(path: str, method: str = "GET", data: Any | None = None) -> Any | None:
    url = f"{OS_URL}/{path.lstrip('/')}"
    body = json.dumps(data).encode() if data is not None else None
    headers = {"Content-Type": "application/json"} if body else {}
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError:
        return None
    except json.JSONDecodeError:
        return None


def os_request_text(path: str) -> str:
    url = f"{OS_URL}/{path.lstrip('/')}"
    try:
        with urllib.request.urlopen(url) as resp:
            return resp.read().decode()
    except urllib.error.URLError:
        return ""


###############################################################################
# Helpers
###############################################################################


def wait_green(timeout: int = 120) -> bool:
    for _ in range(timeout):
        health = os_request("_cluster/health")
        if health and health.get("status") == "green":
            return True
        time.sleep(1)
    status = health.get("status", "unreachable") if health else "unreachable"
    print(f"WARNING: cluster not green after {timeout}s (status: {status})")
    return False


def wait_no_relocating(timeout: int = 120) -> bool:
    for _ in range(timeout):
        health = os_request("_cluster/health")
        if health and health.get("relocating_shards") == 0:
            return True
        time.sleep(1)
    print(f"WARNING: still relocating after {timeout}s")
    return False


def get_all_nodes() -> list[str]:
    """Return all node names in the cluster."""
    nodes = os_request("_cat/nodes?format=json&h=name")
    return [n["name"] for n in nodes] if nodes else []


def get_shards(pattern: str) -> list[dict[str, str]]:
    """Return list of shard records matching the pattern."""
    shards = os_request(f"_cat/shards/{pattern}?format=json")
    return shards if shards else []


def shard_distribution(pattern: str) -> dict[str, int]:
    """Return {node_name: count} for shards matching the pattern, including nodes with 0 shards."""
    dist: dict[str, int] = {node: 0 for node in get_all_nodes()}
    for s in get_shards(pattern):
        node = s.get("node")
        if node:
            dist[node] = dist.get(node, 0) + 1
    return dist


# Print shard-per-node counts for indices matching the pattern (e.g. ".ds-logs-*")
# Sample output:
#     distribution:
#         4 node1
#         4 node2
#         4 node3
def print_distribution(pattern: str, label: str) -> None:
    dist = shard_distribution(pattern)
    print(f"    {label}:")
    for node, count in sorted(dist.items(), key=lambda x: -x[1]):
        print(f"      {count:>4} {node}")


# Verify shards are evenly distributed: every node should hold between
# floor(total_shards / node_count) and ceil(total_shards / node_count). Fails if any shards are unassigned.
def check_ds_balance(pattern: str, label: str) -> None:
    node_count = len(get_all_nodes())
    shards = get_shards(pattern)
    total_shards = len(shards)

    if total_shards == 0:
        fail_test(f"{label} — no shards found")
        return

    unassigned = sum(1 for s in shards if s.get("state") == "UNASSIGNED")

    if unassigned > 0:
        fail_test(f"{label} — {unassigned} UNASSIGNED shards")
        return

    dist = shard_distribution(pattern)
    max_per_node = max(dist.values())
    min_per_node = min(dist.values())
    floor = total_shards // node_count
    cap = ceil_div(total_shards, node_count)

    if min_per_node >= floor and max_per_node <= cap:
        pass_test(f"{label} — {total_shards} shards, min/node={min_per_node}, max/node={max_per_node}, expected=[{floor},{cap}]")
    else:
        fail_test(f"{label} — {total_shards} shards, min/node={min_per_node}, max/node={max_per_node}, expected=[{floor},{cap}]")

    print_distribution(pattern, "distribution")


# Check that new shards were placed on the least-loaded nodes from prev_dist.
def check_placement(prev_dist: dict[str, int], new_dist: dict[str, int], label: str) -> None:
    gained = [node for node in new_dist if new_dist[node] > prev_dist.get(node, 0)]

    info(f"After {label}: {new_dist}")
    ok = True
    for node in gained:
        fewer = sum(1 for n in prev_dist if prev_dist[n] < prev_dist[node])
        if fewer >= len(gained):
            ok = False

    if ok:
        pass_test(f"{label} — new shards went to least-loaded nodes {gained}")
    else:
        fail_test(f"{label} — new shards went to {gained}, prev was {prev_dist}")


def get_shard_node(index: str, shard: int, prirep: str) -> str | None:
    shards = os_request(f"_cat/shards/{index}?format=json")
    if not shards:
        return None
    for s in shards:
        if s.get("shard") == str(shard) and s.get("prirep") == prirep and s.get("state") == "STARTED":
            return s.get("node")
    return None


def move_shard(index: str, shard: int, from_node: str, to_node: str) -> None:
    info(f"Moving {index} [{shard}] from {from_node} to {to_node}")
    result = os_request("_cluster/reroute", method="POST", data={
        "commands": [{
            "move": {
                "index": index,
                "shard": shard,
                "from_node": from_node,
                "to_node": to_node,
            }
        }]
    })
    if result and "error" in result:
        reason = result["error"].get("reason") or result["error"].get("type", "unknown")
        print(f"    ERROR: {reason}")
    wait_no_relocating()


def create_ds_template(name: str) -> None:
    os_request(f"_index_template/{name}-template", method="PUT", data={
        "index_patterns": [name],
        "data_stream": {},
        "template": {
            "settings": {"number_of_shards": 1, "number_of_replicas": 1}
        }
    })


def create_datastream(name: str) -> None:
    os_request(f"{name}/_doc", method="POST", data={
        "@timestamp": "2024-01-01T00:00:00",
        "message": "init"
    })


def rollover_ds(name: str) -> None:
    os_request(f"{name}/_rollover", method="POST")


def delete_datastream(name: str) -> None:
    os_request(f"_data_stream/{name}", method="DELETE")
    os_request(f"_index_template/{name}-template", method="DELETE")


###############################################################################
# Test 1: Basic Shard Placement with Rolling Rollovers
###############################################################################


def test_1() -> None:
    log("TEST 1: Basic Shard Placement")
    print()

    info("Disabling rebalancing...")
    os_request("_cluster/settings", method="PUT", data={
        "persistent": {"cluster.routing.rebalance.enable": "none"}
    })

    create_ds_template("logs")
    create_ds_template("metrics")
    create_datastream("logs")
    create_datastream("metrics")
    time.sleep(3)
    wait_green()

    info("Initial state (1 backing index each, 2 shards each):")
    check_ds_balance(".ds-logs-*", "logs initial")
    check_ds_balance(".ds-metrics-*", "metrics initial")
    print()

    for i in range(1, 6):
        rollover_ds("logs")
        time.sleep(3)
        wait_green()
        check_ds_balance(".ds-logs-*", f"logs after rollover {i}")
    print()

    for i in range(1, 6):
        rollover_ds("metrics")
        time.sleep(3)
        wait_green()
        check_ds_balance(".ds-metrics-*", f"metrics after rollover {i}")
    print()

    info("Final state — both datastreams at 6 backing indices (12 shards each):")
    check_ds_balance(".ds-logs-*", "logs final")
    check_ds_balance(".ds-metrics-*", "metrics final")
    print()


###############################################################################
# Test 2: New Allocation on an Imbalanced Cluster
###############################################################################


def test_2() -> None:
    log("TEST 2: New Allocation on an Imbalanced Cluster")
    print()

    info("Disabling rebalancing...")
    os_request("_cluster/settings", method="PUT", data={
        "persistent": {"cluster.routing.rebalance.enable": "none"}
    })

    create_ds_template("events")
    create_datastream("events")

    for _ in range(5):
        rollover_ds("events")
        time.sleep(2)
    time.sleep(3)
    wait_green()

    info("Initial placement (rebalancing disabled, 6 backing indices, 12 shards):")
    print_distribution(".ds-events-*", "events before force-move")
    print()

    info("Force-moving all primaries → node1, all replicas → node2...")
    shards = get_shards(".ds-events-*")
    indices = sorted(set(s["index"] for s in shards))

    for index in indices:
        primary_node = get_shard_node(index, 0, "p")
        replica_node = get_shard_node(index, 0, "r")

        if primary_node != "node1":
            if primary_node == "node2":
                replica_target = "node3"
            else:
                replica_target = "node2"

            if replica_node != replica_target:
                move_shard(index, 0, replica_node, replica_target)

            move_shard(index, 0, primary_node, "node1")

        replica_node = get_shard_node(index, 0, "r")
        if replica_node != "node2":
            move_shard(index, 0, replica_node, "node2")

    print()
    info("After force-move (expecting node1=6, node2=6, node3=0):")
    print_distribution(".ds-events-*", "events")

    dist = shard_distribution(".ds-events-*")
    if dist.get("node1", 0) != 6 or dist.get("node2", 0) != 6 or dist.get("node3", 0) != 0:
        print(f"{RED}ERROR: test setup failed — expected node1=6, node2=6, node3=0, got {dist}{NC}")
        sys.exit(1)
    info("Test setup verified: node1=6, node2=6, node3=0")
    print()

    info("Creating new datastream 'audit' on imbalanced cluster (rebalancing still disabled)...")
    info("Each rollover adds 2 shards (primary+replica) — they should go to the two least-loaded nodes.")
    print()
    create_ds_template("audit")

    prev_dist = shard_distribution(".ds-*")

    create_datastream("audit")
    time.sleep(3)
    wait_green()
    check_placement(prev_dist, shard_distribution(".ds-*"), "create")

    for i in range(1, 11):
        prev_dist = shard_distribution(".ds-*")
        rollover_ds("audit")
        time.sleep(3)
        wait_green()
        check_placement(prev_dist, shard_distribution(".ds-*"), f"rollover {i}")
    print()


###############################################################################
# Main
###############################################################################


def main() -> None:
    parser = argparse.ArgumentParser(description="Integration tests for datastream allocator")
    parser.add_argument("--no-teardown", action="store_true",
                        help="Leave the cluster running after tests")
    args = parser.parse_args()

    for cmd in ["docker"]:
        if not shutil.which(cmd):
            print(f"Error: {cmd} is required but not installed")
            sys.exit(1)

    plugin_zip = os.path.join(PROJECT_DIR, "build", "distributions", "datastream-allocator-1.0.0.zip")
    if not os.path.isfile(plugin_zip):
        log("Building plugin...")
        subprocess.run(["bash", "build.sh"], cwd=PROJECT_DIR, check=True)

    log("Starting 3-node OpenSearch cluster...")
    subprocess.run(["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "--build"], check=True)

    if not args.no_teardown:
        def cleanup():
            log("Tearing down cluster...")
            subprocess.run(["docker", "compose", "-f", COMPOSE_FILE, "down", "-v"])
        atexit.register(cleanup)
    else:
        log("Teardown disabled (--no-teardown). Cluster will remain running.")

    log("Waiting for cluster to be green...")
    wait_green(180)

    info("Cluster is green. Node list:")
    nodes = os_request_text("_cat/nodes?v&h=name,node.role")
    for line in nodes.splitlines():
        print(f"    {line}")
    print()

    test_1()

    log("Cleaning up test 1...")
    delete_datastream("logs")
    delete_datastream("metrics")
    time.sleep(5)
    wait_green()
    print()

    test_2()

    print()
    print("========================================")
    print(f"Results: {GREEN}{PASS_COUNT} passed{NC}, {RED}{FAIL_COUNT} failed{NC}")
    print("========================================")

    if FAIL_COUNT > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
