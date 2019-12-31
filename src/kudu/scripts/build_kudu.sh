#!/bin/bash

BASE_DIR="$( cd "$( dirname "$0"  )" && cd ../../.. && pwd )"

function usage()
{
    echo "Options:"
    echo "  -h"
    echo "  -g|--custom-gcc"
    exit 0
}

#USAGE: copy_file src [src...] dest
function copy_file() {
  if [[ $# -lt 2 ]]; then
    echo "ERROR: invalid copy file command: cp $*"
    exit 1
  fi  
  cp -v $*
  if [[ $? -ne 0 ]]; then
    echo "ERROR: copy file failed: cp $*"
    exit 1
  fi  
}

function get_stdcpp_lib()
{
    libname=`ldd ${BASE_DIR}/build/latest/bin/kudu 2>/dev/null | grep libstdc++`
    libname=`echo $libname | cut -f1 -d" "`
    if [ $1 = "true" ]; then
        gcc_path=`which gcc`
        echo `dirname $gcc_path`/../lib64/$libname
    else
        libs=(`ldconfig -p|grep $libname|awk '{print $NF}'`)

        for lib in ${libs[*]}; do
            if [ "`check_bit $lib`" = "true" ]; then
                echo "$lib"
                return
            fi
        done;
    fi
}

function get_system_lib()
{
    libname=`ldd ${BASE_DIR}/build/latest/bin/kudu 2>/dev/null | grep "lib${1}\.so"`
    libname=`echo $libname | cut -f1 -d" "`
    libs=(`ldconfig -p|grep $libname|awk '{print $NF}'`)

    bit_mode=`getconf LONG_BIT`
    for lib in ${libs[*]}; do
        if [ "`check_bit $lib`" = "true" ]; then
            echo "$lib"
            return
        fi
    done;

    # if get failed by ldconfig, then just extract lib from ldd result
    libname=`ldd ${BASE_DIR}/build/latest/bin/kudu 2>/dev/null | grep "lib${1}\.so"`
    libname=`echo $libname | cut -f3 -d" "`
    if echo "$libname" | grep -q "lib${2}\.so"; then
        echo "$libname"
    fi
}

function get_system_libname()
{
    libname=`ldd ${BASE_DIR}/build/latest/bin/kudu 2>/dev/null | grep "lib${1}\.so"`
    libname=`echo $libname | cut -f1 -d" "`
    echo "$libname"
}

function check_bit()
{
    bit_mode=`getconf LONG_BIT`
    lib=$1
    check_bit=""
    is_softlink=`file $lib | grep "symbolic link"`

    if [ -z "$is_softlink" ]; then
        check_bit=`file $lib |grep "$bit_mode-bit"`
    else
        real_lib_name=`ls -l $lib |awk '{print $NF}'`
        lib_path=${lib%/*}
        real_lib=${lib_path}"/"${real_lib_name}
        check_bit=`file $real_lib |grep "$bit_mode-bit"`
    fi
    if [ -n "$check_bit" ]; then
        echo "true"
    fi
}

custom_gcc="false"
while [[ $# > 0 ]]; do
    option_key="$1"
    case $option_key in
        -g|--custom-gcc)
            custom_gcc="true"
            ;;
        -h|--help)
            usage
            ;;
    esac
    shift
done

KUDU_VERSION=`cat ${BASE_DIR}/version.txt`
OS=`lsb_release -d | awk '{print $2}'`
echo "Start to build kudu $KUDU_VERSION on $OS"

if [[ "$OS" == "CentOS" ]]; then
  ${BASE_DIR}/build-support/enable_devtoolset.sh
  ${BASE_DIR}/thirdparty/build-if-necessary.sh
elif [[ "$OS" == "Ubuntu" ]]; then
  ${BASE_DIR}/thirdparty/build-if-necessary.sh
else
  echo "ERROR: unsupported OS: $OS in $0"
  exit 1
fi

rm -rf ${BASE_DIR}/build/release
mkdir -p ${BASE_DIR}/build/release
cd ${BASE_DIR}/build/release
../../thirdparty/installed/common/bin/cmake -DCMAKE_BUILD_TYPE=release ../..
make -j `cat /proc/cpuinfo | egrep "^processor\s:" | wc -l`
if [[ $? -ne 0 ]]; then
  echo "ERROR: build Kudu failed"
  exit 1
fi
echo "Build Kudu succeed"

VERSION_DEFINES=${BASE_DIR}/build/release/src/kudu/generated/version_defines.h
if [[ ! -f ${VERSION_DEFINES} ]]; then
  echo "ERROR: $VERSION_DEFINES not found"
  exit 1
fi

CLEAN_REPO=`grep "^#define KUDU_BUILD_CLEAN_REPO " ${VERSION_DEFINES} | awk '{print $NF}' | tr 'A-Z' 'a-z'`
if [[ "$CLEAN_REPO"x != "true"x ]]; then
  echo "ERROR: repository is not clean"
  exit 1
fi

VERSION=`grep "^#define KUDU_VERSION_STRING " ${VERSION_DEFINES} | cut -d "\"" -f 2`
COMMIT_ID=`grep "^#define KUDU_GIT_HASH " ${VERSION_DEFINES} | cut -d "\"" -f 2`
BUILD_TYPE=`grep "^#define KUDU_BUILD_TYPE " ${VERSION_DEFINES} | cut -d "\"" -f 2`
PACK_VERSION=`echo ${VERSION}-${COMMIT_ID:0:7}-${OS}-${BUILD_TYPE} | tr 'A-Z' 'a-z'`
PACK_NAME=kudu-${PACK_VERSION}

echo "Starting package $PACK_NAME"
PACK_DIR=${BASE_DIR}/build/${PACK_NAME}
PACKAGE=${PACK_NAME}.tar.gz
rm -rf ${PACK_DIR} ${BASE_DIR}/build/${PACKAGE}
mkdir -p ${PACK_DIR}
echo "Coping files to $PACK_DIR"
copy_file ${BASE_DIR}/build/latest/bin/kudu-collector ${PACK_DIR}/kudu_collector
copy_file ${BASE_DIR}/build/latest/bin/kudu-master ${PACK_DIR}/kudu_master
copy_file ${BASE_DIR}/build/latest/bin/kudu-tserver ${PACK_DIR}/kudu_tablet_server
copy_file ${BASE_DIR}/build/latest/bin/kudu ${PACK_DIR}/
copy_file `get_stdcpp_lib $custom_gcc` ${PACK_DIR}/
copy_file `get_system_lib crypto` ${PACK_DIR}/`get_system_libname crypto`
copy_file ${BASE_DIR}/src/kudu/scripts/batch_operate_on_tables.sh ${PACK_DIR}/
copy_file ${BASE_DIR}/src/kudu/scripts/falcon_screen.json ${PACK_DIR}/
copy_file ${BASE_DIR}/src/kudu/scripts/falcon_screen.py ${PACK_DIR}/
copy_file ${BASE_DIR}/src/kudu/scripts/kudu_falcon_screen.sh ${PACK_DIR}/
copy_file ${BASE_DIR}/src/kudu/scripts/minos_control_server.py ${PACK_DIR}/
copy_file ${BASE_DIR}/src/kudu/scripts/cal_bill_daily.py ${PACK_DIR}/
copy_file ${BASE_DIR}/src/kudu/scripts/kudu_utils.py ${PACK_DIR}/
copy_file ${BASE_DIR}/src/kudu/scripts/start_kudu.sh ${PACK_DIR}/
copy_file ${BASE_DIR}/src/kudu/scripts/stop_kudu.sh ${PACK_DIR}/
copy_file ${BASE_DIR}/src/kudu/scripts/kudurc ${PACK_DIR}/
copy_file -r ${BASE_DIR}/www ${PACK_DIR}/
cd ${BASE_DIR}/build
tar -czf ${PACKAGE} ${PACK_NAME}
echo "Packaged $PACKAGE succeed"

PACK_TEMPLATE=""
if [[ -n "$MINOS_CONFIG_FILE" ]]; then
  PACK_TEMPLATE=`dirname $MINOS_CONFIG_FILE`/xiaomi-config/package/kudu.yaml
fi

if [[ -f ${PACK_TEMPLATE} ]]; then
  echo "Modifying $PACK_TEMPLATE ..."
  sed -i "/^version:/c version: \"$PACK_VERSION\"" ${PACK_TEMPLATE}
  sed -i "/^build:/c build: \"\.\/run.sh pack\"" ${PACK_TEMPLATE}
  sed -i "/^source:/c source: \"$BASE_DIR/build\"" ${PACK_TEMPLATE}
else
  echo "ERROR: modify kudu.yaml failed"
  exit 1
fi

echo "Done"
