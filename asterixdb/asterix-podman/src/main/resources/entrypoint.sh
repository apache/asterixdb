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
HELPER=${ASTERIX_DIR}/bin/asterixhelper

die() {
  echo "ERROR: $1" >&2
  exit 1
}

# JAVA_OPTS is meant for the server, and a suspending debug agent in it would hang the helper before the server starts
run_helper() {
  JAVA_OPTS="" "$HELPER" "$@"
}

write_credentials() {
  local user=${ASTERIX_USER:-admin} staging
  # Basic auth credentials are split on every ':', so a value containing one could never log in
  [[ "$user" != *:* ]] || die "ASTERIX_USER must not contain ':'"
  [[ "$1" != *:* ]] || die "the password must not contain ':'"
  # add_credential appends to an existing file, and a failure here must leave the current file in place
  staging=$(mktemp -d -p "$PWD") || die "cannot create a temporary directory in $PWD"
  run_helper add_credential -username "$user" -password "$1" -credentialpath "$staging" \
    || die "cannot generate the credential file"
  chmod 600 "$staging/passwd"
  mv -f "$staging/passwd" "$PWD/passwd" || die "cannot replace $PWD/passwd"
  rmdir "$staging"
}

generate_credentials() {
  local generated
  generated=$(run_helper generate_default_credential -credentialpath "$PWD") \
    || die "cannot generate the credential file"
  chmod 600 "$PWD/passwd"
  echo "INFO: Generated password for user admin: $generated"
  echo "INFO: It is only shown on this first start. Set ASTERIX_PASSWORD to replace it."
}

set_up_credentials() {
  local password=$ASTERIX_PASSWORD
  if [ -n "$ASTERIX_PASSWORD" ] && [ -n "$ASTERIX_PASSWORD_FILE" ]; then
    die "ASTERIX_PASSWORD and ASTERIX_PASSWORD_FILE cannot both be set"
  fi
  if [ -n "$ASTERIX_PASSWORD_FILE" ]; then
    password=$(<"$ASTERIX_PASSWORD_FILE") || die "cannot read ASTERIX_PASSWORD_FILE $ASTERIX_PASSWORD_FILE"
    [ -n "$password" ] || die "ASTERIX_PASSWORD_FILE $ASTERIX_PASSWORD_FILE is empty"
  fi
  if [ -n "$password" ]; then
    write_credentials "$password"
  elif [ -n "$ASTERIX_USER" ]; then
    die "ASTERIX_USER needs ASTERIX_PASSWORD or ASTERIX_PASSWORD_FILE"
  elif [ ! -f "$PWD/passwd" ]; then
    generate_credentials
  fi
}

auth_args=()

set_up_auth() {
  case "$ASTERIX_AUTH" in
    "")
      set_up_credentials
      ;;
    none)
      if [ -n "$ASTERIX_USER$ASTERIX_PASSWORD$ASTERIX_PASSWORD_FILE" ]; then
        die "ASTERIX_AUTH=none cannot be combined with ASTERIX_USER, ASTERIX_PASSWORD or ASTERIX_PASSWORD_FILE"
      fi
      echo "WARN: Authentication is disabled: anyone who can reach the API ports can run any statement"
      auth_args=(-api-authentication-enabled false)
      ;;
    *)
      die "ASTERIX_AUTH must be none or unset, not $ASTERIX_AUTH"
      ;;
  esac
}

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
  set_up_auth
  # the server's child processes include UDF interpreters, which must not inherit the password
  unset ASTERIX_AUTH ASTERIX_USER ASTERIX_PASSWORD ASTERIX_PASSWORD_FILE
  # only arguments before --nc reach both the CC and the NC
  exec "$ASTERIX_DIR/bin/asterixccnc" "-config-file" "$BAKED_CONF_FILE" "${auth_args[@]}" "--nc" "-node-id" "1" "-cluster-address" "127.0.0.1" "${orig_args[@]}"
else
  echo "Configuration supplied, bypassing defaults"
  unset ASTERIX_AUTH ASTERIX_USER ASTERIX_PASSWORD ASTERIX_PASSWORD_FILE
  exec "$ASTERIX_DIR/bin/asterixccnc" "${orig_args[@]}"
fi
