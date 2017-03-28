#
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
#
$MANAGIX_HOME/bin/managix stop -n nc1 1>/dev/null 2>&1;
$MANAGIX_HOME/bin/managix delete -n nc1 1>/dev/null 2>&1;
$MANAGIX_HOME/bin/managix create -n nc1 -c $MANAGIX_HOME/clusters/local/local.xml;
$MANAGIX_HOME/bin/managix stop -n nc1;
cp $MANAGIX_HOME/../../../asterix-external-data/target/asterix-external-data-*-tests.jar \
    $MANAGIX_HOME/clusters/local/working_dir/asterix/repo/
$MANAGIX_HOME/bin/managix start -n nc1;
