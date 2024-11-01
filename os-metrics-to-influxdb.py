#! /usr/bin/env python3
"""A simple script for collecting CPU/memory utilisation metrics locally and pass them to InfluxDB"""

import argparse
import json
import logging
import os
import sys

import requests
from psutil import cpu_percent, virtual_memory
# from psutil import disk_partitions, disk_usage    # there is a plan to add them in the future 


def env_default(env_var: str, default_val: any = None) -> dict:
    """A quick solution to manage env variable with argparse.
       Some idea taken from https://stackoverflow.com/questions/10551117 .
    """
    return (
        {"default": os.environ.get(env_var)}
        if os.environ.get(env_var)
        else {"default": default_val}
    )


def parse_args() -> argparse.Namespace:
    """Parses the script arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--verbose",
        required=False,
        help="turn on verbose output",
        action="store_true",
    )
    parser.add_argument(
        "--influxdb_url",
        required=True,
        help=(
            "InfluxDB URL"
        ),
        type=str,
        default=None,
    )
    parser.add_argument(
        "--influxdb_org",
        required=True,
        help="organisation name that the bucket is created for in InluxDB",
        type=str,
        default="",
    )
    parser.add_argument(
        "--influxdb_bucket",
        required=True,
        help="InfluxDB bucket where data will be injected in",
        type=str,
        default="",
    )
    parser.add_argument(
        "--influxdb_token",
        help="InfluxDB token with writing permission to the bucket",
        **env_default("INFLUXDB_TOKEN"),
    )
    parser.add_argument(
        "--influxdb_rate_limit",
        required=False,
        help="number of data sent with a single POST request (kiB). Default: 100 kiB'",
        type=int,
        default=100
    )
    parser.add_argument(
        "--influxdb_gzip",
        required=False,
        help="turns on the data compression.",
        action="store_true",
    )
    parser.add_argument(
        "--request_timeout",
        help="the timeout for HTTP requests [s]",
        **env_default("REQUEST_TIMEOUT", 5),
    )
    return parser.parse_args()


def get_org_id(org_name: str, master_token: str, request_timeout: int = 5) -> str:
    """Get orgID based on organisation name provided."""
    org_url = f"http://localhost:8086/api/v2/orgs?org={org_name}"
    org_response = requests.get(
        org_url,
        timeout=request_timeout,
        headers={
            "Authorization": f"Token {master_token}",
            "Content-type": "application/json",
        },
    )
    logging.debug(
        f"get_org_id, full InfluxDB response: {json.dumps(org_response.json(), indent=4)}"
    )
    if org_response.status_code != 200:
        logging.error(
            f"Unexpected response - code: {org_response.status_code}, message: {json.dumps(org_response.json(), indent=4)}"
        )
        sys.exit()
    # checks if organisation list is empty
    elif not bool(org_response.json()["orgs"]):
        logging.error(
            f"Organisation name may be incorrect: {org_name}, or you have no right to retrieve organisation info in InfluxDB."
        )
        sys.exit()
    return str(org_response.json()["orgs"][0]["id"])


args: argparse.Namespace = parse_args()

if args.verbose:
    logging.getLogger().setLevel(logging.DEBUG)



# Collects CPU total and physical memory utilisation [ % ]
cpu_util: float = cpu_percent()
memory_util: float = virtual_memory().percent

# Puts collected metrics into the list wrapping them with Influx line protocol format.
metric_line_proto: list = [
    f"cpu_utilisation,host=ansible percent_usage={cpu_util}",
    f"memory_utilisation,host=ansible percent_usage={memory_util}"
]


url: str = f"{args.influxdb_url}/api/v2/write?org={args.influxdb_org}&bucket={args.influxdb_bucket}&precision=s"
logging.debug(f"InfluxDB API URL: {url}")

request_headers: dict = {"Authorization": f"Token {args.influxdb_token}", 
                "Content-type": "text/plain; charset=utf-8", 
                "Accept": "application/json"}
request_headers.update({"Content-Encoding": "gzip"}) if args.influxdb_gzip else None
line: str = ""
to_send: str = ""
counter: int = 0
lines_to_send: int = len(metric_line_proto)
for line in metric_line_proto:
    lines_to_send -= 1
    counter += len(line)
    to_send += f"{line}\n"
    if counter >= args.influxdb_rate_limit*1024 or lines_to_send == 0:
        response = requests.post(
                        url, 
                        timeout=int(args.request_timeout), 
                        headers=request_headers,
                        data=to_send)
        print(response.status_code, response.content)
        counter = 0
        to_send = ""        