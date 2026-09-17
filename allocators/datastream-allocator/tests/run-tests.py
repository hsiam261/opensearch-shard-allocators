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


def os_request(path: str, method: str = "GET", data: Any = None) -> Any:
    url = f"{OS_URL}/{path.lstrip('/')}"
    body = json.dumps(data).encode() if data is not None else None
    headers = {"Content-Type": "application/json"} if body else {}
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode())
        except (json.JSONDecodeError, Exception):
            return None
    except (urllib.error.URLError, ConnectionError):
        return None
    except json.JSONDecodeError:
        return None



###############################################################################
# Helpers
###############################################################################


def wait_green(timeout: int = 120) -> bool:
    health = None
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
    if node_count == 0:
        fail_test(f"{label} — no nodes found")
        return

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


# Check placement: per-datastream balance is the primary criterion, total shard count breaks ties.
# 1. No skipped node should have had strictly fewer per-datastream shards than any gained node.
# 2. Among nodes tied in per-datastream count, gained nodes should not have had more total shards
#    than any skipped node.
def check_placement(prev_dist: dict[str, int], new_dist: dict[str, int],
                    prev_total: dict[str, int], label: str) -> None:
    gained = set(node for node in new_dist if new_dist[node] > prev_dist.get(node, 0))

    if not gained:
        fail_test(f"{label} — no nodes gained shards")
        return

    max_prev_gained = max(prev_dist.get(n, 0) for n in gained)

    info(f"After {label}: ds={new_dist}")
    ok = True
    reason = ""

    # No node with fewer per-datastream shards than the most-loaded gained node should be skipped
    for node in prev_dist:
        if node not in gained and prev_dist[node] < max_prev_gained:
            ok = False
            reason = (f"skipped {node} (ds={prev_dist[node]}) had fewer per-datastream "
                      f"shards than gained max (ds={max_prev_gained})")
            break

    if ok:
        max_total_gained = max(prev_total.get(n, 0) for n in gained)

        # Since the above passed, all skipped nodes have per-datastream shards >= max_prev_gained.
        # The only remaining error is a wrong tiebreak: skipping a node with the same per-datastream
        # count but fewer total shards than a gained node.
        for node in prev_dist:
            if (node not in gained
                    and prev_dist[node] == max_prev_gained
                    and prev_total.get(node, 0) < max_total_gained):
                ok = False
                reason = (f"skipped {node} (total={prev_total[node]}) had fewer total shards "
                          f"than gained node (total={max_total_gained}) at same ds count")
                break

    if ok:
        pass_test(f"{label} — placement correct {sorted(gained)}")
    else:
        fail_test(f"{label} — {reason}")


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


def create_ds_template(name: str) -> Any:
    return os_request(f"_index_template/{name}-template", method="PUT", data={
        "index_patterns": [name],
        "data_stream": {},
        "template": {
            "settings": {"number_of_shards": 1, "number_of_replicas": 1}
        }
    })


def create_datastream(name: str) -> Any:
    return os_request(f"{name}/_doc", method="POST", data={
        "@timestamp": "2024-01-01T00:00:00",
        "message": "init"
    })


def rollover_ds(name: str) -> Any:
    return os_request(f"{name}/_rollover", method="POST")


def delete_datastream(name: str) -> tuple[Any, Any]:
    ds = os_request(f"_data_stream/{name}", method="DELETE")
    tpl = os_request(f"_index_template/{name}-template", method="DELETE")
    return ds, tpl


###############################################################################
# Test 1: Basic Shard Placement with Rolling Rollovers
###############################################################################


def test_1() -> None:
    log("TEST 1: Basic Shard Placement")
    print()

    info("Disabling rebalancing...")
    result = os_request("_cluster/settings", method="PUT", data={
        "persistent": {"cluster.routing.rebalance.enable": "none"}
    })
    if not result or not result.get("acknowledged"):
        fail_test(f"failed to disable rebalancing: {result}")
        return

    for ds in ["logs", "metrics"]:
        result = create_ds_template(ds)
        if not result or not result.get("acknowledged"):
            fail_test(f"failed to create {ds} template: {result}")
            return

    for ds in ["logs", "metrics"]:
        result = create_datastream(ds)
        if not result or "error" in result:
            fail_test(f"failed to create {ds} datastream: {result}")
            return
    time.sleep(3)
    if not wait_green():
        fail_test("cluster not green after creating datastreams")
        return

    info("Initial state (1 backing index each, 2 shards each):")
    check_ds_balance(".ds-logs-*", "logs initial")
    check_ds_balance(".ds-metrics-*", "metrics initial")
    print()

    for i in range(1, 6):
        result = rollover_ds("logs")
        if not result or not result.get("rolled_over"):
            fail_test(f"failed to rollover logs (rollover {i}): {result}")
            return
        time.sleep(3)
        if not wait_green():
            fail_test(f"cluster not green after logs rollover {i}")
            return
        check_ds_balance(".ds-logs-*", f"logs after rollover {i}")
    print()

    for i in range(1, 6):
        result = rollover_ds("metrics")
        if not result or not result.get("rolled_over"):
            fail_test(f"failed to rollover metrics (rollover {i}): {result}")
            return
        time.sleep(3)
        if not wait_green():
            fail_test(f"cluster not green after metrics rollover {i}")
            return
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
    result = os_request("_cluster/settings", method="PUT", data={
        "persistent": {"cluster.routing.rebalance.enable": "none"}
    })
    if not result or not result.get("acknowledged"):
        fail_test(f"failed to disable rebalancing: {result}")
        return

    result = create_ds_template("events")
    if not result or not result.get("acknowledged"):
        fail_test(f"failed to create events template: {result}")
        return

    result = create_datastream("events")
    if not result or "error" in result:
        fail_test(f"failed to create events datastream: {result}")
        return

    for _ in range(5):
        result = rollover_ds("events")
        if not result or not result.get("rolled_over"):
            fail_test(f"failed to rollover events: {result}")
            return
        time.sleep(2)
    time.sleep(3)
    if not wait_green():
        fail_test("cluster not green after events setup")
        return

    info("Initial placement (rebalancing disabled, 6 backing indices, 12 shards):")
    print_distribution(".ds-events-*", "events before force-move")
    print()

    info("Force-moving all primaries → node1, all replicas → node2...")
    shards = get_shards(".ds-events-*")
    indices = sorted(set(s["index"] for s in shards))

    for index in indices:
        primary_node = get_shard_node(index, 0, "p")
        replica_node = get_shard_node(index, 0, "r")

        if primary_node is None or replica_node is None:
            print(f"{RED}ERROR: could not find STARTED shards for {index}{NC}")
            sys.exit(1)

        if primary_node != "node1":
            if primary_node == "node2":
                replica_target = "node3"
            else:
                replica_target = "node2"

            if replica_node != replica_target:
                move_shard(index, 0, replica_node, replica_target)

            move_shard(index, 0, primary_node, "node1")

        replica_node = get_shard_node(index, 0, "r")
        if replica_node is None:
            print(f"{RED}ERROR: could not find STARTED replica for {index}{NC}")
            sys.exit(1)

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
    result = create_ds_template("audit")
    if not result or not result.get("acknowledged"):
        fail_test(f"failed to create audit template: {result}")
        return

    prev_dist = shard_distribution(".ds-audit-*")
    prev_total = shard_distribution(".ds-*")

    result = create_datastream("audit")
    if not result or "error" in result:
        fail_test(f"failed to create audit datastream: {result}")
        return
    time.sleep(3)
    if not wait_green():
        fail_test("cluster not green after creating audit datastream")
        return
    check_placement(prev_dist, shard_distribution(".ds-audit-*"), prev_total, "create")

    for i in range(1, 11):
        prev_dist = shard_distribution(".ds-audit-*")
        prev_total = shard_distribution(".ds-*")
        result = rollover_ds("audit")
        if not result or not result.get("rolled_over"):
            fail_test(f"failed to rollover audit (rollover {i}): {result}")
            return
        time.sleep(3)
        if not wait_green():
            fail_test(f"cluster not green after audit rollover {i}")
            return
        check_placement(prev_dist, shard_distribution(".ds-audit-*"), prev_total, f"rollover {i}")
    print()


###############################################################################
# Main
###############################################################################


def main() -> None:
    parser = argparse.ArgumentParser(description="Integration tests for datastream allocator")
    parser.add_argument("--no-teardown", action="store_true",
                        help="Leave the cluster running after tests")
    args = parser.parse_args()

    if not shutil.which("docker"):
        print("Error: docker is required but not installed")
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
    if not wait_green(180):
        print(f"{RED}ERROR: cluster did not reach green status{NC}")
        sys.exit(1)

    info("Cluster is green. Node list:")
    nodes = os_request("_cat/nodes?format=json&h=name,node.role")
    if nodes:
        for n in nodes:
            print(f"    {n['name']}  {n.get('node.role', '')}")
    print()

    test_1()

    log("Cleaning up test 1...")
    for ds in ["logs", "metrics"]:
        ds_result, tpl_result = delete_datastream(ds)
        if not ds_result or not ds_result.get("acknowledged"):
            print(f"{RED}WARNING: failed to delete datastream {ds}: {ds_result}{NC}")
        if not tpl_result or not tpl_result.get("acknowledged"):
            print(f"{RED}WARNING: failed to delete template {ds}: {tpl_result}{NC}")
    time.sleep(5)
    if not wait_green():
        print(f"{RED}WARNING: cluster not green after cleanup{NC}")
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
