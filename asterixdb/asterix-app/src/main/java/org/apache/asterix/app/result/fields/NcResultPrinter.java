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
import java.util.List;

import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.message.ExecuteStatementResponseMessage;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.ResultSetId;

public class NcResultPrinter implements IResponseFieldPrinter {

    private final IStatementExecutor.ResultDelivery delivery;
    private final ExecuteStatementResponseMessage responseMsg;
    private final IApplicationContext appCtx;
    private final IResultSet resultSet;
    private final SessionOutput sessionOutput;
    private final IStatementExecutor.Stats stats;

    public NcResultPrinter(IApplicationContext appCtx, ExecuteStatementResponseMessage responseMsg,
            IResultSet resultSet, IStatementExecutor.ResultDelivery delivery, SessionOutput sessionOutput,
            IStatementExecutor.Stats stats) {
        this.appCtx = appCtx;
        this.responseMsg = responseMsg;
        this.delivery = delivery;
        this.resultSet = resultSet;
        this.sessionOutput = sessionOutput;
        this.stats = stats;
    }

    @Override
    public void print(PrintWriter pw) throws HyracksDataException {
        IStatementExecutor.ResultMetadata resultMetadata = responseMsg.getMetadata();
        List<Triple<JobId, ResultSetId, ARecordType>> resultSets = resultMetadata.getResultSets();
        if (delivery == IStatementExecutor.ResultDelivery.IMMEDIATE && !resultSets.isEmpty()) {
            for (int i = 0; i < resultSets.size(); i++) {
                Triple<JobId, ResultSetId, ARecordType> rsmd = resultSets.get(i);
                ResultReader resultReader = new ResultReader(resultSet, rsmd.getLeft(), rsmd.getMiddle());
                ResultUtil.printResults(appCtx, resultReader, sessionOutput, stats, rsmd.getRight());
                if (i + 1 != resultSets.size()) {
                    ResponsePrinter.printFieldSeparator(pw);
                }
            }
        } else {
            pw.append(responseMsg.getResult());
        }
    }

    @Override
    public String getName() {
        return ResultsPrinter.FIELD_NAME;
    }
}
