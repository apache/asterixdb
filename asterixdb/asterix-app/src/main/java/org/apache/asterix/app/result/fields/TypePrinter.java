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

import static org.apache.hyracks.http.server.utils.HttpUtil.ContentType.CSV;

import java.io.PrintWriter;

import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.translator.SessionConfig;
import org.apache.hyracks.http.server.utils.HttpUtil;

public class TypePrinter implements IResponseFieldPrinter {

    private static final String FIELD_NAME = "type";
    private final SessionConfig sessionConfig;

    public TypePrinter(SessionConfig sessionConfig) {
        this.sessionConfig = sessionConfig;
    }

    @Override
    public void print(PrintWriter pw) {
        switch (sessionConfig.fmt()) {
            case ADM:
                ResultUtil.printField(pw, FIELD_NAME, HttpUtil.ContentType.APPLICATION_ADM, false);
                break;
            case CSV:
                String contentType =
                        CSV + "; header=" + (sessionConfig.is(SessionConfig.FORMAT_CSV_HEADER) ? "present" : "absent");
                ResultUtil.printField(pw, FIELD_NAME, contentType, false);
                break;
            default:
                break;
        }
    }

    @Override
    public String getName() {
        return FIELD_NAME;
    }
}
