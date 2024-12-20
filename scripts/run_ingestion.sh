#!/bin/bash

APP_DIR="/opt/dataCEVA/data-ingestion"
SCRIPTS_DIR="$APP_DIR/scripts"
LOGS_DIR="$APP_DIR/logs"

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")

LOG_FILE="$LOGS_DIR/run_${TIMESTAMP}.log"

mkdir -p "$LOGS_DIR"
if [ $? -ne 0 ]; then
    echo "Failed to create logs directory at $LOGS_DIR."
    exit 1
fi 

exec > "$LOG_FILE" 2>&1

echo "=== Data Ingestion Script Execution started at $(date) ==="

source "$APP_DIR/data/venv/bin/activate"
if [ $? -ne 0 ]; then
    echo "Failed to activate venv"
    exit 1
fi

echo "Virtual environment activated."

cd "$APP_DIR/data" || { echo "Failed to navigate to app directory at $APP_DIR/data"; exit 1; }
echo "Navigated to application directory."

echo "Running Python Ingestion Script..."

if [ "$1" == "weekly" ]; then
    echo "Running Python Ingestion Script with -w flag..."
    python download_csv_convert_avro.py -w
elif [ "$1" == "neighborhood" ]; then
    echo "Running Python Ingestion Script with -n flag..."
    python download_csv_convert_avro.py -n
else
    echo "Invalid or no argument provided. Expected 'weekly' or 'neighborhood'."
    exit 1
fi

PYTHON_EXIT_CODE=$?

if [ "$1" == "weekly" ]; then
    echo "Starting kafka producer Script with -w flag..."
    python send_to_kafka.py -w
elif [ "$1" == "neighborhood" ]; then
    echo "Starting kafka producer Script with -n flag..."
    python send_to_kafka.py -n
else
    echo "Invalid or no argument provided. Expected 'weekly' or 'neighborhood'."
    exit 1
fi

deactivate

echo "VENV deactivated"

if [ PYTHON_EXIT_CODE -ne 0 ]; then
    echo "Python script exited with code $PYTHON_EXIT_CODE"
    exit $PYTHON_EXIT_CODE
fi

echo "=== Data Ingestion Script Execution succesfully completed at $(date) ==="

