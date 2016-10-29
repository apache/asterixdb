/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.common.utils;

public class ServletUtil {

    public enum Servlets {
        AQL("/aql"),
        AQL_QUERY("/query"),
        AQL_UPDATE("/update"),
        AQL_DDL("/ddl"),
        SQLPP("/sqlpp"),
        SQLPP_QUERY("/query/sqlpp"),
        SQLPP_UPDATE("/update/sqlpp"),
        SQLPP_DDL("/ddl/sqlpp"),
        QUERY_STATUS("/query/status"),
        QUERY_RESULT("/query/result"),
        QUERY_SERVICE("/query/service"),
        CONNECTOR("/connector"),
        SHUTDOWN("/admin/shutdown"),
        VERSION("/admin/version"),
        CLUSTER_STATE("/admin/cluster/*"),
        CLUSTER_STATE_NODE_DETAIL("/admin/cluster/node/*"),
        CLUSTER_STATE_CC_DETAIL("/admin/cluster/cc/*"),
        DIAGNOSTICS("/admin/diagnostics");

        private final String path;

        Servlets(String path) {
            this.path = path;
        }

        public String getPath() {
            return path;
        }

    }

    private ServletUtil() {
        throw new AssertionError("No objects of this class should be created.");
    }
}
