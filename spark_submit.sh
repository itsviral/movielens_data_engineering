#!/bin/bash

# Ensure the log directory exists
mkdir -p logs

# Define the path to your Python script
PYTHON_SCRIPT="src/main.py"

# Define the location of your Spark installation if not using a managed cluster
#SPARK_HOME=/path/to/your/spark/home

# Define the application name
APP_NAME="MovieLensDataEngineering"

# Define the master URL (YARN, Kubernetes, Mesos, etc.)
MASTER_URL="local[*] "  # Could be "yarn", "k8s://<kubernetes-master>", "mesos://<mesos-master>", or "spark://<spark-master>"

# Define deploy mode (client or cluster) :: In local if cluster is not running use client
DEPLOY_MODE="client"  # Use "cluster" for production

# Set driver and executor memory
DRIVER_MEMORY="4G"
EXECUTOR_MEMORY="2G"

# Set the number of executor cores and number of executors
EXECUTOR_CORES=2
NUM_EXECUTORS=7

# Define additional Spark configurations as needed
SPARK_CONF=(
    --conf "spark.executor.memoryOverhead=1024"
    --conf "spark.driver.maxResultSize=2G"
    --conf "spark.sql.shuffle.partitions=200"
    --conf "spark.dynamicAllocation.enabled=true"
    --conf "spark.dynamicAllocation.initialExecutors=10"
    --conf "spark.dynamicAllocation.maxExecutors=50"
    --conf "spark.dynamicAllocation.minExecutors=10"
    --conf "spark.yarn.executor.memoryOverhead=1024"
    --conf "spark.yarn.am.memory=2G"
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/log4j.properties"
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$(pwd)/log4j.properties"
)

# Submit the Spark job :: In some case you may hve to use ${SPARK_HOME}/bin/
spark-submit \
    --master ${MASTER_URL} \
    --deploy-mode ${DEPLOY_MODE} \
    --name ${APP_NAME} \
    --driver-memory ${DRIVER_MEMORY} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    --num-executors ${NUM_EXECUTORS} \
    "${SPARK_CONF[@]}" \
    $PYTHON_SCRIPT
