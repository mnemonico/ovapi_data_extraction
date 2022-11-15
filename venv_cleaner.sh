#!/usr/bin/env bash
set -e -u -x
pip freeze | grep -v -f requirements.txt - | grep -v '^#' | xargs pip uninstall -y
