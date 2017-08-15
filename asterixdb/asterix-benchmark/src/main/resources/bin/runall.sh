#!/bin/bash
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

# Gets the absolute path so that the script can work no matter where it is invoked.
pushd `dirname $0` > /dev/null
SCRIPT_PATH=`pwd -P`
popd > /dev/null

# Gets the perf. distribution path.
DIST_PATH=`dirname "${SCRIPT_PATH}"`

# Gets the benchmark path.
BENCHMARK_PATH=$DIST_PATH/benchmarks

# Gets the inventory file path.
INVENTORY=$1

# Gets the system name.
SYSTEM_NAME=$2

# Checks the existence of the inventory file.
if [ ! -f "$INVENTORY" ];
then
   echo "The inventory file \"$INVENTORY\" does not exist."
   exit 1
fi

ANSIBLE_PATH=$DIST_PATH/ansible

# Enumerates all benchmarks and run each of them.
for benchmark in $BENCHMARK_PATH/*; do
    # Sets up paths for scripts.
    gen=$benchmark/gen
    setup=$benchmark/setup
    load=$benchmark/load
    queries=$benchmark/queries
    teardown=$benchmark/teardown

    # Creates datasets that are used for testing.
    ansible-playbook -i $INVENTORY --extra-vars="inventory=${INVENTORY}" $setup/setup.yml

    # Generates data distributedly on all machines
    ansible-playbook -i $INVENTORY $gen/gen.yml

    # Loads generated data into those created datasets.
    ansible-playbook -i $INVENTORY --extra-vars="inventory=${INVENTORY}" $load/load.yml

    # Runs queries for heating the system.
    for query in $queries/*.sqlpp; do
      ansible-playbook -i $INVENTORY --extra-vars="query_file=${query} report=false" \
        $ANSIBLE_PATH/runquery.yml
    done

    # Runs all queries three times and collect numbers.
    for number in 1 2 3
    do
        for query in $queries/*.sqlpp; do
           ansible-playbook -i $INVENTORY --extra-vars="query_file=${query} report=true metric='${SYSTEM_NAME}'" \
                 $ANSIBLE_PATH/runquery.yml
        done
    done

    # Removes all loaded datasets.
    ansible-playbook -i $INVENTORY  --extra-vars="inventory=${INVENTORY}" $teardown/teardown.yml
done
