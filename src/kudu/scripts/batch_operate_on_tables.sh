#!/bin/bash

if [ $# -lt 3 ]
then
  echo "This tool is for batch operation on batch of tables in a cluster"
  echo "USAGE: $0 file operate cluster [dst-cluster]"
  echo "        file: A file contains several table names in a cluster, one table name per line."
  echo "              Or 'auto' means all tables in this cluster"
  echo "     operate: Now support 'copy', 'delete', 'describe' and 'scan'"
  echo "     cluster: CLuster name or master RPC addresses"
  echo " dst-cluster: Master addresses of destination cluster, needed only when 'operate' is 'copy'"
  exit -1
fi

FILE=$1
OPERATE=$2
CLUSTER=$3
DST_CLUSTER=$4
#FLAGS="-show_attributes"
#FLAGS="-create_table=false -write_type=upsert"
BIN_PATH=${KUDU_HOME}/kudu
PID=$$

echo "UID: ${UID}"
echo "PID: ${PID}"
echo "tables:"
if [ "${FILE}" == "auto" ]
then
    echo "All tables in the cluster"
else
    cat ${FILE}
fi
echo "operate: ${OPERATE}"
echo "cluster: ${CLUSTER}"
echo "dst cluster: ${DST_CLUSTER}"
echo "flags: ${FLAGS}"

echo ""
echo "All params above have been checked? (yes)"
read INPUT
if [ ! -n "${INPUT}" ] || [ "${INPUT}" != "yes" ]
then
    exit $?
fi

if [ -n "${DST_CLUSTER}" ]
then
    DST_CLUSTER=@${DST_CLUSTER}
fi

if [ "${FILE}" == "auto" ]
then
    TABLE_LIST=/tmp/$UID.${PID}.table.list
    ${BIN_PATH} table list @${CLUSTER} | sort -n >${TABLE_LIST}
else
    TABLE_LIST=${FILE}
fi

if [ ! -f "${TABLE_LIST}" ]
then
    echo "file ${TABLE_LIST} is not exist!"
    exit $?
fi

while read TABLE
do
    ${BIN_PATH} table ${OPERATE} @${CLUSTER} ${TABLE} ${DST_CLUSTER} ${FLAGS}
done < ${TABLE_LIST}
