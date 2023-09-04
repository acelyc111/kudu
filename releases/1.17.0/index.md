---
title: Apache Kudu release 1.17.0
layout: single_col
active_nav: download
single_col_extra_classes: releases
---

<!--

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

-->

## Apache Kudu release 1.17.0

See the [Kudu 1.17.0 Release Notes](docs/release_notes.html).

Downloads of Kudu 1.17.0 are available in the following formats:

* [Kudu 1.17.0 source tarball](http://www.apache.org/dyn/closer.cgi?path=kudu/1.17.0/apache-kudu-1.17.0.tar.gz)
  ([SHA512](https://www.apache.org/dist/kudu/1.17.0/apache-kudu-1.17.0.tar.gz.sha512),
  [Signature](https://www.apache.org/dist/kudu/1.17.0/apache-kudu-1.17.0.tar.gz.asc))

You can use the [KEYS file](https://www.apache.org/dist/kudu/KEYS) to verify the included GPG signature.

To verify the integrity of the release, check the following:

* Verify the checksum by downloading the release and the `.sha512` file, and
  running the following command:
    * On Linux: `sha512sum -c apache-kudu-1.17.0.tar.gz.sha512`
    * On MacOS: `shasum -a 512 -c apache-kudu-1.17.0.tar.gz.sha512`
* Verify the signature by downloading the release and the `.asc` file, and
  doing the following:
    * Import the KEYS file to the GPG keychain by running `gpg --import KEYS`
    * Run `gpg --verify apache-kudu-1.17.0.tar.gz.asc apache-kudu-1.17.0.tar.gz`

Additional links:

* [Kudu 1.17.0 Documentation](docs/)
* [Kudu 1.17.0 Java API docs](apidocs/)
* [Kudu 1.17.0 C++ API docs](cpp-client-api/)