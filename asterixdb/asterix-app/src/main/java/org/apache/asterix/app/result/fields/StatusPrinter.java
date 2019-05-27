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
package org.apache.asterix.app.result.fields;

import java.io.PrintWriter;

import org.apache.asterix.api.http.server.AbstractQueryApiServlet;
import org.apache.asterix.common.api.IResponseFieldPrinter;

public class StatusPrinter implements IResponseFieldPrinter {

    public static final String FIELD_NAME = "status";
    private final AbstractQueryApiServlet.ResultStatus status;

    public StatusPrinter(AbstractQueryApiServlet.ResultStatus status) {
        this.status = status;
    }

    @Override
    public void print(PrintWriter pw) {
        pw.append("\t\"").append(FIELD_NAME).append("\": \"").append(status.str()).append("\"");
    }

    @Override
    public String getName() {
        return FIELD_NAME;
    }
}
