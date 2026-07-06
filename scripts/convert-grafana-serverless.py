#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import os
import sys


REMOVED_LABELS = {"k8s_cluster", "sharedpool_id", "tidb_cluster"}
SERVERLESS_DASHBOARD_TITLE = "TiDB-Serverless-TiCDC-New-Arch"
CONTROL_PLANE_DATASOURCE_INPUT = "DS_TEST-CLUSTER-VARIABLES"
CONTROL_PLANE_DATASOURCE = "${" + CONTROL_PLANE_DATASOURCE_INPUT + "}"
CONTROL_PLANE_QUERY = 'label_values({application_id="devtier-infra"}, control_plane_info)'


def default_output_path(input_path):
    base, ext = os.path.splitext(input_path)
    if not ext:
        ext = ".json"
    return f"{base}_serverless{ext}"


def split_matchers(selector):
    matchers = []
    start = 0
    in_string = False
    escaped = False
    for idx, char in enumerate(selector):
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if char == "," and not in_string:
            matchers.append(selector[start:idx])
            start = idx + 1
    matchers.append(selector[start:])
    return matchers


def matcher_name(matcher):
    stripped = matcher.strip()
    for op in ("=~", "!~", "!=", "="):
        op_idx = stripped.find(op)
        if op_idx > 0:
            return stripped[:op_idx].strip()
    return ""


def rewrite_selector(selector):
    matchers = split_matchers(selector)
    kept = []
    removed = False
    for matcher in matchers:
        if matcher_name(matcher) in REMOVED_LABELS:
            removed = True
            continue
        kept.append(matcher.strip())
    if not removed:
        return "{" + selector + "}"
    if not kept:
        return ""
    return "{" + ",".join(kept) + "}"


def find_selector_end(query, start):
    in_string = False
    escaped = False
    for idx in range(start + 1, len(query)):
        char = query[idx]
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if char == "}" and not in_string:
            return idx
    return -1


def rewrite_promql(query):
    if not any(label in query for label in REMOVED_LABELS):
        return query

    parts = []
    pos = 0
    while pos < len(query):
        start = query.find("{", pos)
        if start == -1:
            parts.append(query[pos:])
            break
        end = find_selector_end(query, start)
        if end == -1:
            parts.append(query[pos:])
            break
        parts.append(query[pos:start])
        parts.append(rewrite_selector(query[start + 1 : end]))
        pos = end + 1
    return "".join(parts)


def rewrite_strings(value):
    if isinstance(value, dict):
        return {key: rewrite_strings(item) for key, item in value.items()}
    if isinstance(value, list):
        return [rewrite_strings(item) for item in value]
    if isinstance(value, str):
        return rewrite_promql(value)
    return value


def ensure_control_plane_datasource_input(dashboard):
    datasource_input = {
        "name": CONTROL_PLANE_DATASOURCE_INPUT,
        "label": CONTROL_PLANE_DATASOURCE,
        "description": "",
        "type": "datasource",
        "pluginId": "prometheus",
        "pluginName": "Prometheus",
    }
    inputs = dashboard.setdefault("__inputs", [])
    for idx, item in enumerate(inputs):
        if item.get("name") == CONTROL_PLANE_DATASOURCE_INPUT:
            inputs[idx] = datasource_input
            return
    inputs.append(datasource_input)


def control_plane_variable():
    variable = {
        "allValue": None,
        "current": {
            "isNone": True,
            "selected": False,
            "text": "None",
            "value": "",
        },
        "datasource": CONTROL_PLANE_DATASOURCE,
        "definition": CONTROL_PLANE_QUERY,
        "description": None,
        "error": None,
        "hide": 0,
        "includeAll": False,
        "label": "Region",
        "multi": False,
        "name": "control_plane_info",
        "options": [],
        "query": {
            "query": CONTROL_PLANE_QUERY,
            "refId": "local-control_plane_info-Variable-Query",
        },
        "refresh": 1,
        "regex": "",
        "skipUrlSync": False,
        "sort": 1,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False,
    }
    return variable


def convert_dashboard(dashboard):
    dashboard = rewrite_strings(dashboard)
    dashboard["title"] = SERVERLESS_DASHBOARD_TITLE
    ensure_control_plane_datasource_input(dashboard)
    templating = dashboard.setdefault("templating", {})
    variables = templating.setdefault("list", [])
    variables = [
        item
        for item in variables
        if item.get("name") not in REMOVED_LABELS
        and item.get("name") != "control_plane_info"
    ]
    templating["list"] = [control_plane_variable()] + variables
    return dashboard


def collect_strings(value):
    if isinstance(value, dict):
        for item in value.values():
            yield from collect_strings(item)
        return
    if isinstance(value, list):
        for item in value:
            yield from collect_strings(item)
        return
    if isinstance(value, str):
        yield value


def validate_dashboard(dashboard):
    if dashboard.get("title") != SERVERLESS_DASHBOARD_TITLE:
        raise ValueError("dashboard title must be set to the serverless test title")

    inputs = dashboard.get("__inputs", [])
    if not any(item.get("name") == CONTROL_PLANE_DATASOURCE_INPUT for item in inputs):
        raise ValueError("control plane datasource input is missing")

    variables = dashboard.get("templating", {}).get("list", [])
    if not variables or variables[0].get("name") != "control_plane_info":
        raise ValueError("control_plane_info must be the first templating variable")
    if variables[0].get("datasource") != CONTROL_PLANE_DATASOURCE:
        raise ValueError("control_plane_info must use the test cluster variables datasource")
    for item in variables:
        if item.get("name") in REMOVED_LABELS:
            raise ValueError(f"removed variable still exists: {item['name']}")

    for value in collect_strings(dashboard):
        for label in REMOVED_LABELS:
            if label in value:
                raise ValueError(f"removed label still exists in: {value}")
        if "{}" in value:
            raise ValueError(f"empty selector still exists in: {value}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert a Grafana dashboard JSON to the serverless variant."
    )
    parser.add_argument("input", help="Source Grafana dashboard JSON")
    parser.add_argument(
        "output",
        nargs="?",
        help="Output dashboard JSON. Defaults to INPUT with _serverless before .json.",
    )
    args = parser.parse_args()

    output_path = args.output or default_output_path(args.input)
    with open(args.input, "r", encoding="utf-8") as input_file:
        dashboard = json.load(input_file)

    dashboard = convert_dashboard(dashboard)
    validate_dashboard(dashboard)

    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as output_file:
        json.dump(dashboard, output_file, indent=2)
        output_file.write("\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
