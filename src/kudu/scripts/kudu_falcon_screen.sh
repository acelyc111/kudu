#!/bin/bash

PID=$$
BASE_DIR="$( cd "$( dirname "$0" )" && pwd )"
KUDU=${KUDU_HOME}/kudu
KUDURC=${KUDU_CONFIG}/kudurc
if [[ ! -f ${KUDURC} ]]; then
  echo "ERROR: ${KUDURC} not found"
  exit 1
fi

function usage() {
cat << EOF
This tool is for update falcon screen for specified kudu cluster.
USAGE: $0 <cluster_name>
       cluster_name       Cluster name operated on, should be configurated in $KUDU_CONFIG/kudurc
EOF
}

if [[ $# -lt 1 ]]
then
  usage
  exit 1
fi

CLUSTER=$1

echo "UID: ${UID}"
echo "PID: ${PID}"
echo "cluster: ${CLUSTER}"
echo "Start time: `date`"
ALL_START_TIME=$((`date +%s`))
echo

# get master list
${KUDU} master list @${CLUSTER} -format=space | awk -F' |:' '{print $2}' | sort -n &>/tmp/${UID}.${PID}.kudu.master.list
if [[ $? -ne 0 ]]; then
    echo "`kudu master list @${CLUSTER} -format=space` failed"
    exit $?
fi

MASTER_COUNT=`cat /tmp/${UID}.${PID}.kudu.master.list | wc -l`
if [[ ${MASTER_COUNT} -eq 0 ]]; then
    echo "ERROR: master list is empty, please check the cluster ${CLUSTER}"
    exit -1
fi

# get tserver list
${KUDU} tserver list @${CLUSTER} -format=space | awk -F' |:' '{print $2}' | sort -n &>/tmp/${UID}.${PID}.kudu.tserver.list
if [[ $? -ne 0 ]]; then
    echo "`kudu tserver list @${CLUSTER} -format=space` failed"
    exit $?
fi

TSERVER_COUNT=`cat /tmp/${UID}.${PID}.kudu.tserver.list | wc -l`
if [[ ${TSERVER_COUNT} -eq 0 ]]; then
    echo "ERROR: tserver list is empty, please check the cluster ${CLUSTER}"
    exit 1
fi

# get table list
${KUDU} table list @${CLUSTER} | sort -n &>/tmp/${UID}.${PID}.kudu.table.list
echo "total `wc -l /tmp/${UID}.${PID}.kudu.table.list | awk '{print $1}'` tables to monitor"

python ${BASE_DIR}/falcon_screen.py ${CLUSTER} ${BASE_DIR}/falcon_screen.json /tmp/${UID}.${PID}.kudu.master.list /tmp/${UID}.${PID}.kudu.tserver.list /tmp/${UID}.${PID}.kudu.table.list
if [[ $? -ne 0 ]]; then
    echo "ERROR: falcon screen operate failed"
    exit 1
fi

echo
echo "Finish time: `date`"
ALL_FINISH_TIME=$((`date +%s`))
echo "Falcon screen operate done, elapsed time is $((ALL_FINISH_TIME - ALL_START_TIME)) seconds."

rm -f /tmp/${UID}.${PID}.kudu.* &>/dev/null
