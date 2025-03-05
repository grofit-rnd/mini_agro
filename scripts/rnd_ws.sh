#!/bin/bash
# check if the last directory is "grofit_rnd"
source miniagro/scripts/base_ws.sh
if [ $? -ne 0 ]; then
  echo "Error: The base_ws.sh script failed to run."
  return 1
fi

CURRENT_DIR=$(pwd)
# build the Python path with the current directory and API server subdirectory
PP="$CURRENT_DIR"

# export GROFIT_RND_CONFIG_FILE='rnd_server.json'
# export GROFIT_DM_CONFIG_FILE='dm_server.json'
# export GROFIT_API_SERVER_NAME="agro_data"
# export GROFIT_DB_NAME="agro_data_db"

# export GROFIT_CONFIG_FILE=$GROFIT_RND_CONFIG_FILE

# export the Python path
export PYTHONPATH=$PP
