#!/bin/bash

OS=`lsb_release -d | awk '{print $2}'`
echo "Setup build env for kudu on $OS"

if [[ "$OS" == "CentOS" ]]; then
  sudo yum -y install autoconf automake cyrus-sasl-devel cyrus-sasl-gssapi \
    cyrus-sasl-plain flex gcc gcc-c++ gdb git java-1.8.0-openjdk-devel \
    krb5-server krb5-workstation libtool make openssl-devel patch pkgconfig \
    redhat-lsb-core rsync unzip vim-common which
  DTLS_RPM=rhscl-devtoolset-3-epel-6-x86_64-1-2.noarch.rpm
  DTLS_RPM_URL=https://www.softwarecollections.org/repos/rhscl/devtoolset-3/epel-6-x86_64/noarch/${DTLS_RPM}
  wget ${DTLS_RPM_URL} -O ${DTLS_RPM}
  sudo yum install -y scl-utils ${DTLS_RPM}
  sudo yum install -y devtoolset-3-toolchain
elif [[ "$OS" == "Ubuntu" ]]; then
  sudo apt-get -y install autoconf automake curl flex g++ gcc gdb git \
    krb5-admin-server krb5-kdc krb5-user libkrb5-dev libsasl2-dev libsasl2-modules \
    libsasl2-modules-gssapi-mit libssl-dev libtool lsb-release make ntp \
    openjdk-8-jdk openssl patch pkg-config python rsync unzip vim-common
else
  echo "Unsupported OS: $OS in $0"
  exit
fi
