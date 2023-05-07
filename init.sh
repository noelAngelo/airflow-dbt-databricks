#!/bin/bash

# Load environment variables from .env file
if [[ -f .env ]]; then
  echo "Loading environment variables from .env file..."

  while read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^[[:alnum:]_]+= ]]; then
      export "$line"
    fi
  done < .env

else
  echo ".env file not found."
  exit 1
fi

# Use the environment variables
echo " - PROJ_DIR: $PROJ_DIR"
echo " - AIRFLOW_UID: $AIRFLOW_UID"
echo " - _AIRFLOW_WWW_USER_USERNAME: $_AIRFLOW_WWW_USER_USERNAME"