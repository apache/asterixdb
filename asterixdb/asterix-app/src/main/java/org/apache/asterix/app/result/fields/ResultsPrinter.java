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

import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ResultsPrinter implements IResponseFieldPrinter {

    public static final String FIELD_NAME = "results";
    private final IApplicationContext appCtx;
    private final ARecordType recordType;
    private final ResultReader resultReader;
    private final IStatementExecutor.Stats stats;
    private final SessionOutput sessionOutput;

    public ResultsPrinter(IApplicationContext appCtx, ResultReader resultReader, ARecordType recordType,
            IStatementExecutor.Stats stats, SessionOutput sessionOutput) {
        this.appCtx = appCtx;
        this.recordType = recordType;
        this.resultReader = resultReader;
        this.stats = stats;
        this.sessionOutput = sessionOutput;
    }

    @Override
    public void print(PrintWriter pw) throws HyracksDataException {
        ResultUtil.printResults(appCtx, resultReader, sessionOutput, stats, recordType);
    }

    @Override
    public String getName() {
        return FIELD_NAME;
    }
}
