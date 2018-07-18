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
package org.apache.asterix.translator;

import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class BaseClientRequest implements IClientRequest {
    protected final IStatementExecutorContext ctx;
    protected final String contextId;
    private boolean complete;

    public BaseClientRequest(IStatementExecutorContext ctx, String contextId) {
        this.ctx = ctx;
        this.contextId = contextId;
    }

    @Override
    public synchronized void complete() {
        if (complete) {
            return;
        }
        complete = true;
        ctx.remove(contextId);
    }

    @Override
    public synchronized void cancel(ICcApplicationContext appCtx) throws HyracksDataException {
        if (complete) {
            return;
        }
        complete();
        doCancel(appCtx);
    }

    protected abstract void doCancel(ICcApplicationContext appCtx) throws HyracksDataException;
}
