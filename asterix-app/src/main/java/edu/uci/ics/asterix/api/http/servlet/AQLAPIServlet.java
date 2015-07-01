/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.api.http.servlet;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import edu.uci.ics.asterix.aql.base.Statement.Kind;

public class AQLAPIServlet extends RESTAPIServlet {

    private static final long serialVersionUID = 1L;

    private static final String AQL_STMT_PARAM_NAME = "aql";

    private static final List<Kind> allowedStatements = new ArrayList<>();

    static {
        for (Kind k : Kind.values()) {
            allowedStatements.add(k);
        }
    }

    @Override
    protected String getQueryParameter(HttpServletRequest request) {
        return request.getParameter(AQL_STMT_PARAM_NAME);
    }

    @Override
    protected List<Kind> getAllowedStatements() {
        return allowedStatements;
    }

    @Override
    protected String getErrorMessage() {
        throw new IllegalStateException();
    }

}
