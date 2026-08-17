#!/bin/bash
set -e

echo "[$(date)] InvestBot container ready (PYTHONPATH=${PYTHONPATH:-/app})"

if [ "$#" -gt 0 ]; then
  exec "$@"
fi

exec sleep infinity
