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

# - Find LZ4 (lz4.h, liblz4.a, liblz4.so or liblz4.dylib)
# This module defines
#  LZ4_INCLUDE_DIR, directory containing headers
#  LZ4_STATIC_LIB, path to liblz4's static library
#  LZ4_FOUND, whether lz4 has been found

find_path(LZ4_INCLUDE_DIR lz4.h
  # make sure we don't accidentally pick up a different version
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(LZ4_STATIC_LIB liblz4.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

set(__CURRENT_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
if (APPLE)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".dylib")
else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
endif()
find_library(LZ4_SHARED_LIB lz4
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${__CURRENT_FIND_LIBRARY_SUFFIXES})
unset(__CURRENT_FIND_LIBRARY_SUFFIXES)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Lz4 REQUIRED_VARS
  LZ4_SHARED_LIB LZ4_STATIC_LIB LZ4_INCLUDE_DIR)
