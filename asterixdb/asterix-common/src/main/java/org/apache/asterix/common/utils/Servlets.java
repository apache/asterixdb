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

public class Servlets {

    public static final String AQL = "/aql";
    public static final String AQL_QUERY = "/query";
    public static final String AQL_UPDATE = "/update";
    public static final String AQL_DDL = "/ddl";
    public static final String SQLPP = "/sqlpp";
    public static final String SQLPP_QUERY = "/query/sqlpp";
    public static final String SQLPP_UPDATE = "/update/sqlpp";
    public static final String SQLPP_DDL = "/ddl/sqlpp";
    public static final String QUERY_STATUS = "/query/service/status/*";
    public static final String QUERY_RESULT = "/query/service/result/*";
    public static final String QUERY_SERVICE = "/query/service";
    public static final String QUERY_AQL = "/query/aql";
    public static final String CONNECTOR = "/connector";
    public static final String REBALANCE = "/admin/rebalance";
    public static final String SHUTDOWN = "/admin/shutdown";
    public static final String VERSION = "/admin/version";
    public static final String RUNNING_REQUESTS = "/admin/requests/running/*";
    public static final String CLUSTER_STATE = "/admin/cluster/*";
    public static final String CLUSTER_STATE_NODE_DETAIL = "/admin/cluster/node/*";
    public static final String CLUSTER_STATE_CC_DETAIL = "/admin/cluster/cc/*";
    public static final String DIAGNOSTICS = "/admin/diagnostics";
    public static final String ACTIVE_STATS = "/admin/active/*";

    private Servlets() {
    }
}
