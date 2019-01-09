#!/bin/bash

PID=$$
BASE_DIR="$( cd "$( dirname "$0" )" && pwd )"
KUDU=${KUDU_HOME}/kudu
COLLECTOR=${KUDU_HOME}/kudu_collector
if [[ ! -f ${KUDU} || ! -f ${COLLECTOR} ]]; then
  echo "ERROR: ${KUDU} or ${COLLECTOR} not found"
  exit 1
fi
KUDURC=${KUDU_CONFIG}/kudurc
if [[ ! -f ${KUDURC} ]]; then
  echo "ERROR: ${KUDURC} not found"
  exit 1
fi

function usage() {
cat << EOF
This tool is for update falcon screen for specified kudu cluster.
USAGE: $0 <cluster_name> [table_count] [metrics_template]
       cluster_name       Cluster name operated on, should be configurated in $KUDU_CONFIG/kudurc
       table_count        An indicator of how many tables will be monitored, actual monitored table count is in range [table_count, 3*table_count]
       metrics_template   Which metric template will be used, 'simple' or 'full'
EOF
}

if [[ $# -lt 1 ]]
then
  usage
  exit 1
fi

CLUSTER=$1
TABLE_COUNT=9999
if [[ $# -ge 2 ]]
then
  TABLE_COUNT=$2
fi

TEMPLATE_NAME='full'
if [[ $# -ge 3 ]]
then
  TEMPLATE_NAME=$3
fi
if [[ "${TEMPLATE_NAME}"x != "simple"x && "${TEMPLATE_NAME}"x != "full"x ]]
then
  usage
  exit 1
fi

echo "UID: ${UID}"
echo "PID: ${PID}"
echo "cluster: ${CLUSTER}"
echo "top n table: ${TABLE_COUNT}"
echo "metric template: ${TEMPLATE_NAME}"
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

function parse_yaml() {
  python -c "import yaml;print(yaml.load(open('$1').read(), Loader=yaml.FullLoader)['clusters_info']['$2']['master_addresses'])"
}
MASTERS=$(parse_yaml ${KUDURC} ${CLUSTER})

# get table list
${COLLECTOR} -collector_master_addrs=${MASTERS} -collector_cluster_name=${CLUSTER} -collector_report_method=local -collector_metrics=bytes_flushed,on_disk_size,scanner_bytes_returned -log_dir=./log > /tmp/${UID}.${PID}.kudu.metric_table_value
if [[ $? -ne 0 ]]; then
    echo "ERROR: ${COLLECTOR} execute failed"
    exit 1
fi

cat /tmp/${UID}.${PID}.kudu.metric_table_value | egrep "^table bytes_flushed " | sort -rnk4 | head -n ${TABLE_COUNT} | awk '{print $3}' > /tmp/${UID}.${PID}.kudu.top.bytes_flushed
cat /tmp/${UID}.${PID}.kudu.metric_table_value | egrep "^table on_disk_size " | sort -rnk4 | head -n ${TABLE_COUNT} | awk '{print $3}' > /tmp/${UID}.${PID}.kudu.top.on_disk_size
cat /tmp/${UID}.${PID}.kudu.metric_table_value | egrep "^table scanner_bytes_returned " | sort -rnk4 | head -n ${TABLE_COUNT} | awk '{print $3}' > /tmp/${UID}.${PID}.kudu.top.scanner_bytes_returned
cat /tmp/${UID}.${PID}.kudu.top.* | sort -n | uniq > /tmp/${UID}.${PID}.kudu.table.list
echo "total `wc -l /tmp/${UID}.${PID}.kudu.table.list | awk '{print $1}'` tables to monitor"
echo -e "\033[32m Please set the following one line to the kudu collector's \`collector_attributes\` argument manually\033[0m"
echo -n "table_name:"
awk BEGIN{RS=EOF}'{gsub(/\n/,",");print}' /tmp/${UID}.${PID}.kudu.table.list
echo ""

python ${BASE_DIR}/falcon_screen.py ${CLUSTER} ${TEMPLATE_NAME} ${BASE_DIR}/falcon_screen.json /tmp/${UID}.${PID}.kudu.master.list /tmp/${UID}.${PID}.kudu.tserver.list /tmp/${UID}.${PID}.kudu.table.list
if [[ $? -ne 0 ]]; then
    echo "ERROR: falcon screen operate failed"
    exit 1
fi

echo
echo "Finish time: `date`"
ALL_FINISH_TIME=$((`date +%s`))
echo "Falcon screen operate done, elapsed time is $((ALL_FINISH_TIME - ALL_START_TIME)) seconds."

rm -f /tmp/${UID}.${PID}.kudu.* &>/dev/null
