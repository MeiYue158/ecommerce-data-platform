#!/bin/bash
export SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-2g}"
export SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-2}"

/opt/spark/sbin/start-worker.sh "${SPARK_MASTER_URL:-spark://spark-master:7077}"

# Keep container running and tail the log
tail -f /opt/spark/logs/spark-*-org.apache.spark.deploy.worker.Worker-*.out
