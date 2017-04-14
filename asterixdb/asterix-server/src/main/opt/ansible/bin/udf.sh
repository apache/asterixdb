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

# Get options
usage() { echo "./udf.sh -m [i|u] -d DATAVERSE_NAME -l LIBRARY_NAME [-p UDF_PACKAGE_PATH]" 1>&2; exit 1; }

while getopts ":d:l:p:m:" o; do
  case "${o}" in
    m)
        mode=${OPTARG}
        ;;
    d)
        dname=${OPTARG}
        ;;
    l)
        libname=${OPTARG}
        ;;
    p)
        tarpath=${OPTARG}
        ;;
    *)
        echo $o
        usage
        ;;
  esac
done
shift $((OPTIND-1))

if [[ -z $dname ]] || [[ -z $libname ]]; then
    echo "Dataverse name or Library name is missing"
fi

# Gets the absolute path so that the script can work no matter where it is invoked.
pushd `dirname $0` > /dev/null
SCRIPT_PATH=`pwd -P`
popd > /dev/null
ANSB_PATH=`dirname "${SCRIPT_PATH}"`

INVENTORY=$ANSB_PATH/conf/inventory

# Deploy/Destroy the UDF package on all nodes.
export ANSIBLE_HOST_KEY_CHECKING=false
if [[ $mode = "i" ]]; then
    if [[ -z $tarpath ]]; then
        echo "UDF package path is undefined"
    else
        echo "Install library to $dname.$libname"
        ansible-playbook -i $INVENTORY $ANSB_PATH/yaml/deploy_udf.yml --extra-vars "dataverse=$dname libname=$libname package_path=$tarpath"
    fi
elif [[ $mode == "u" ]]; then
    echo "Uninstall library in $dname.$libname"
    ansible-playbook -i $INVENTORY $ANSB_PATH/yaml/destroy_udf.yml --extra-vars "dataverse=$dname libname=$libname"
else
    echo "Wrong mode"
fi