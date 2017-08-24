# ------------------------------------------------------------
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
# ------------------------------------------------------------

#!/bin/bash

pushd `dirname $0` > /dev/null
SCRIPT_PATH=`pwd -P`
popd > /dev/null
export ANSIBLE_HOST_KEY_CHECKING=false
export ANSIBLE_SSH_RETRIES=3

if [[ "$1" = /* ]]; then
    INVENTORY=$1
else
    INVENTORY=$SCRIPT_PATH/$1
fi

SYSTEM_NAME=$2

if [ -z "$SYSTEM_NAME" ];
then
    SYSTEM_NAME="SparkSQL"
fi

# Checks the existence of the inventory file.
if [ ! -f "$INVENTORY" ];
then
   echo "The inventory file \"$INVENTORY\" does not exist."
   exit 1
fi
# Load data
ansible-playbook -i $INVENTORY $SCRIPT_PATH/../../benchmarks/tpch/gen/gen.yml
# Configure HDFS
ansible-playbook -i $INVENTORY $SCRIPT_PATH/ansible/install_hdfs.yml
ansible-playbook -i $INVENTORY $SCRIPT_PATH/ansible/start_hdfs.yml
# Configure Spark
ansible-playbook -i $INVENTORY $SCRIPT_PATH/ansible/install_spark.yml
ansible-playbook -i $INVENTORY $SCRIPT_PATH/ansible/start_spark.yml
ansible-playbook -i $INVENTORY $SCRIPT_PATH/ansible/load_tpch.yml
# Execute queries
ansible-playbook -i $INVENTORY --extra-vars="metric='${SYSTEM_NAME}'" $SCRIPT_PATH/ansible/prepare_queries.yml
ansible-playbook -i $INVENTORY $SCRIPT_PATH/ansible/execute_queries.yml