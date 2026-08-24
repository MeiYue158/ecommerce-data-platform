#!/bin/bash
export SPARK_MASTER_HOST="${SPARK_MASTER_HOST:-spark-master}"
export SPARK_MASTER_PORT="${SPARK_MASTER_PORT:-7077}"
export SPARK_MASTER_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT:-8080}"

/opt/spark/sbin/start-master.sh

# Keep container running and tail the log
tail -f /opt/spark/logs/spark-*-org.apache.spark.deploy.master.Master-*.out
