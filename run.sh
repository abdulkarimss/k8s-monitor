#!/bin/bash
# Launch K8s Monitor TUI
cd "$(dirname "$0")"
exec .venv/bin/python3 k8s_monitor.py "$@"
