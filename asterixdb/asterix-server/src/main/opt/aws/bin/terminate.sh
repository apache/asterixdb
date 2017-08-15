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
AWS_PATH=`dirname "${SCRIPT_PATH}"`

# Terminates an AWS cluster

if [ -z "$1" ]
then
    # without name parameter.
    ansible-playbook -i "localhost," $AWS_PATH/yaml/aws_terminate.yml
else
    # with name parameter
    ansible-playbook -i "localhost," --extra-vars="tag=${1}" $AWS_PATH/yaml/aws_terminate.yml
fi