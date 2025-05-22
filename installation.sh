#!/bin/bash
set -x

logger() {
  currentTS=$(date '+%Y.%m.%d-%H:%M:%S %Z')
  printf "%s %s: %s\n" "$currentTS" "$1" "$2"
}

set -e

# get vars
argTypeServeur="" # gcp ou dauphine

while [ $# -gt 0 ]; do
  key="$1"
  case $key in
    -s|--typeServeur)
      typeServeur="$2"
      shift # past argument
      shift # past value
      ;;
    *) # unknown option
      shift
      ;;
  esac
done

logger "INFO" "Get args: "
argTypeServeur=${typeServeur}

logger "INFO" "Launch with arguments: "

if [ $argTypeServeur = "local" ]
then
  spark-submit \
  --master spark://MacBook-Pro-de-Mathis.home:7077 \
  --deploy-mode cluster \
  --conf spark.driver.cores=2 \
  --conf spark.driver.memory=4G \
  --conf spark.executor.instances=3 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=8G \
  --class com.dauphine.flight.Application \
  /Users/mathisperez/DevApp/ProjetFlight/target/ProjetFlight-1.0.0-SNAPSHOT.jar ${argTypeServeur}
else
  spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.driver.cores=2 \
  --conf spark.driver.memory=4G \
  --conf spark.executor.instances=3 \
  --conf spark.executor.cores=5 \
  --conf spark.executor.memory=10G \
  --class com.dauphine.flight.Application \
  ProjetFlight-1.0.0-SNAPSHOT.jar ${argTypeServeur}
fi
