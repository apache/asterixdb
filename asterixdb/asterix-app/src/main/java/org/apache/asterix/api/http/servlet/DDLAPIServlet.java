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

import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;

public class DDLAPIServlet extends RESTAPIServlet {
    private static final long serialVersionUID = 1L;

    public DDLAPIServlet(ILangCompilationProvider compilationProvider) {
        super(compilationProvider);
    }

    @Override
    protected String getQueryParameter(HttpServletRequest request) {
        return request.getParameter("ddl");
    }

    @Override
    protected List<Byte> getAllowedStatements() {
        Byte[] statementsArray = { Statement.DATAVERSE_DECL, Statement.DATAVERSE_DROP, Statement.DATASET_DECL,
                Statement.NODEGROUP_DECL, Statement.NODEGROUP_DROP, Statement.TYPE_DECL, Statement.TYPE_DROP,
                Statement.CREATE_INDEX, Statement.INDEX_DECL, Statement.CREATE_DATAVERSE, Statement.DATASET_DROP,
                Statement.INDEX_DROP, Statement.CREATE_FUNCTION, Statement.FUNCTION_DROP, Statement.CREATE_PRIMARY_FEED,
                Statement.CREATE_SECONDARY_FEED, Statement.DROP_FEED, Statement.CREATE_FEED_POLICY,
                Statement.DROP_FEED_POLICY };
        return Arrays.asList(statementsArray);
    }

    @Override
    protected String getErrorMessage() {
        return "Invalid statement: Non-DDL statement %s to the DDL API.";
    }
}
