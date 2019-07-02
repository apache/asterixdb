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

package org.apache.asterix.app.message;

import java.util.Collection;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;

public final class ExecuteStatementResponseMessage implements INcAddressedMessage {
    private static final long serialVersionUID = 1L;

    private final long requestMessageId;

    private String result;

    private IStatementExecutor.ResultMetadata metadata;

    private IStatementExecutor.Stats stats;

    private Throwable error;

    private ExecutionPlans executionPlans;

    private Collection<Warning> warnings;

    public ExecuteStatementResponseMessage(long requestMessageId) {
        this.requestMessageId = requestMessageId;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        NCMessageBroker mb = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        MessageFuture future = mb.deregisterMessageFuture(requestMessageId);
        if (future != null) {
            future.complete(this);
        }
    }

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public IStatementExecutor.ResultMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(IStatementExecutor.ResultMetadata metadata) {
        this.metadata = metadata;
    }

    public IStatementExecutor.Stats getStats() {
        return stats;
    }

    public void setStats(IStatementExecutor.Stats stats) {
        this.stats = stats;
    }

    public ExecutionPlans getExecutionPlans() {
        return executionPlans;
    }

    public void setExecutionPlans(ExecutionPlans executionPlans) {
        this.executionPlans = executionPlans;
    }

    public Collection<Warning> getWarnings() {
        return warnings;
    }

    public void setWarnings(Collection<Warning> warnings) {
        this.warnings = warnings;
    }

    @Override
    public String toString() {
        return String.format("%s(id=%s): %d characters", getClass().getSimpleName(), requestMessageId,
                result != null ? result.length() : 0);
    }
}
