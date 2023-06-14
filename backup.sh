#!/bin/bash

SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR"
source env/bin/activate
python3 backupBaseServer.py
