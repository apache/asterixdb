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
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ExplainOnlyResultsPrinter implements IResponseFieldPrinter {

    private final IApplicationContext appCtx;
    private final String plan;
    private final SessionOutput sessionOutput;

    public ExplainOnlyResultsPrinter(IApplicationContext appCtx, String plan, SessionOutput sessionOutput) {
        this.appCtx = appCtx;
        this.plan = plan;
        this.sessionOutput = sessionOutput;
    }

    @Override
    public void print(PrintWriter pw) throws HyracksDataException {
        boolean quoteResult = sessionOutput.config().getPlanFormat() == SessionConfig.PlanFormat.STRING;
        sessionOutput.config().set(SessionConfig.FORMAT_QUOTE_RECORD, quoteResult);
        ResultUtil.printResults(appCtx, plan, sessionOutput, new IStatementExecutor.Stats(), null);
    }

    @Override
    public String getName() {
        return ResultsPrinter.FIELD_NAME;
    }
}
