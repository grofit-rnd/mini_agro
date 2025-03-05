#!/bin/bash

# get the current directory
CURRENT_DIR=$(pwd)
# check if the last directory is "mini_agro"
if [[ "${CURRENT_DIR##*/}" != "miniagro" ]]; then
  echo "Error: The current directory's last pack must be 'miniagro'."
  return 1
fi

echo $CURRENT_DIR/_env/miniagro/bin/activate

# activate the Python virtual environment
source $CURRENT_DIR/_env/miniagro/bin/activate

# build the Python path with the current directory and API server subdirectory
export GROFIT_BASE_DIR=$CURRENT_DIR
export PYTHONPATH=$CURRENT_DIR

