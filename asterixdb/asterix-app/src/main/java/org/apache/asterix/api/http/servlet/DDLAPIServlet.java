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
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;

public class DDLAPIServlet extends RESTAPIServlet {
    private static final long serialVersionUID = 1L;

    private static final List<Byte> allowedStatements = Collections.unmodifiableList(Arrays.asList(
            Statement.Kind.DATAVERSE_DECL, Statement.Kind.DATAVERSE_DROP, Statement.Kind.DATASET_DECL,
            Statement.Kind.NODEGROUP_DECL, Statement.Kind.NODEGROUP_DROP, Statement.Kind.TYPE_DECL,
            Statement.Kind.TYPE_DROP, Statement.Kind.CREATE_INDEX, Statement.Kind.INDEX_DECL,
            Statement.Kind.CREATE_DATAVERSE, Statement.Kind.DATASET_DROP, Statement.Kind.INDEX_DROP,
            Statement.Kind.CREATE_FUNCTION, Statement.Kind.FUNCTION_DROP, Statement.Kind.CREATE_PRIMARY_FEED,
            Statement.Kind.CREATE_SECONDARY_FEED, Statement.Kind.DROP_FEED, Statement.Kind.CREATE_FEED_POLICY,
            Statement.Kind.DROP_FEED_POLICY));

    public DDLAPIServlet(ILangCompilationProvider compilationProvider) {
        super(compilationProvider);
    }

    @Override
    protected String getQueryParameter(HttpServletRequest request) {
        return request.getParameter("ddl");
    }

    @Override
    protected List<Byte> getAllowedStatements() {
        return allowedStatements;
    }

    @Override
    protected String getErrorMessage() {
        return "Invalid statement: Non-DDL statement %s to the DDL API.";
    }
}
