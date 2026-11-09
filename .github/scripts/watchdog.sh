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

# Usage: watchdog.sh <timeout-minutes> <idle-timeout-minutes> <build-log>
#
# Hung-build guard, meant to be started in the background. Once the build has run for <timeout-minutes>,
# or <build-log> has not grown for <idle-timeout-minutes> (0 disables either limit), every JVM gets a
# thread dump in target/threaddumps and is then killed. The job's own timeout-minutes would kill the
# JVMs without leaving a trace of where they were stuck, and too late for later steps to archive anything.

set -u

timeout_secs=$(( $1 * 60 ))
idle_secs=$(( $2 * 60 ))
log=$3
dumps=${GITHUB_WORKSPACE:-$PWD}/target/threaddumps
start=$(date +%s)

touch "$log"
while sleep 15; do
  now=$(date +%s)
  if (( timeout_secs > 0 && now - start >= timeout_secs )); then
    reason="exceeded the $1 minute build timeout"
  elif (( idle_secs > 0 && now - $(stat -c %Y "$log") >= idle_secs )); then
    reason="produced no output for $2 minutes"
  else
    continue
  fi
  echo "::error::Build $reason; thread dumps were written to target/threaddumps and the build was killed"
  mkdir -p "$dumps"
  for pid in $(pgrep -x java); do
    jcmd "$pid" Thread.print -l > "$dumps/threaddump-$pid-$now.txt" 2>&1
  done
  pkill -TERM -x java
  sleep 20
  pkill -KILL -x java
  exit 0
done
