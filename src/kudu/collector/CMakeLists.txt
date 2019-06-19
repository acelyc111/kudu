# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#########################################
# collector
#########################################

set(COLLECTOR_SRCS
  collector.cc
  collector_options.cc
)

add_library(collector ${COLLECTOR_SRCS})
target_link_libraries(collector
  kserver
  log
  security
  server_process)

#########################################
# kudu-collector
#########################################

add_executable(kudu-collector collector_main.cc)
target_link_libraries(kudu-collector
  ${SANITIZER_OPTIONS_OVERRIDE}
  ${KRB5_REALM_OVERRIDE}
  collector
  ${KUDU_BASE_LIBS})

option(KUDU_COLLECTOR_INSTALL "Whether to install the Kudu Tablet Server executable" ON)
if(KUDU_COLLECTOR_INSTALL)
  install(TARGETS kudu-collector RUNTIME DESTINATION ${CMAKE_INSTALL_SBINDIR})
else()
  message(STATUS "Skipping install rule for the Kudu Collector executable")
endif()

