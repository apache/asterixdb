#!/usr/bin/env bash
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
BAKED_CONF_FILE=${ASTERIX_DIR}/conf/cc.conf

orig_args=("$@")
num_args=$#
user_config=0

while [[ "$num_args" -gt 0 ]]
do
  arg="$1"
  if [ $arg = "-config-file" ]; then
      user_config=1
  fi
  shift
  ((num_args--))
done

cd "$HOME" || exit

if (( user_config == 0)); then
  echo "Using default configuration"
  exec "$ASTERIX_DIR/bin/asterixccnc" "-config-file" "$BAKED_CONF_FILE" "--nc" "-node-id" "1" "-cluster-address" "127.0.0.1" "${orig_args[@]}"
else
  echo "Configuration supplied, bypassing defaults"
  exec "$ASTERIX_DIR/bin/asterixccnc" "${orig_args[@]}"
fi
