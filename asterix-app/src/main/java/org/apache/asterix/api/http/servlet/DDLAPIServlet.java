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
package org.apache.asterix.api.http.servlet;

import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.base.Statement.Kind;

public class DDLAPIServlet extends RESTAPIServlet {
    private static final long serialVersionUID = 1L;

    protected String getQueryParameter(HttpServletRequest request) {
        return request.getParameter("ddl");
    }

    protected List<Statement.Kind> getAllowedStatements() {
        Kind[] statementsArray = { Kind.DATAVERSE_DECL, Kind.DATAVERSE_DROP, Kind.DATASET_DECL, Kind.NODEGROUP_DECL,
                Kind.NODEGROUP_DROP, Kind.TYPE_DECL, Kind.TYPE_DROP, Kind.CREATE_INDEX, Kind.INDEX_DECL,
                Kind.CREATE_DATAVERSE, Kind.DATASET_DROP, Kind.INDEX_DROP, Kind.CREATE_FUNCTION, Kind.FUNCTION_DROP,
                Kind.CREATE_PRIMARY_FEED, Kind.CREATE_SECONDARY_FEED, Kind.DROP_FEED, Kind.CREATE_FEED_POLICY, Kind.DROP_FEED_POLICY };
        return Arrays.asList(statementsArray);
    }

    protected String getErrorMessage() {
        return "Invalid statement: Non-DDL statement %s to the DDL API.";
    }
}
