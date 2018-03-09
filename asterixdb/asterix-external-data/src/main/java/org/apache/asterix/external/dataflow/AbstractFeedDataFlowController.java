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
package org.apache.asterix.external.dataflow;

import java.io.Closeable;
import java.io.IOException;

import org.apache.asterix.external.api.IDataFlowController;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public abstract class AbstractFeedDataFlowController implements IDataFlowController, Closeable {
    protected TupleForwarder tupleForwarder;
    protected final IHyracksTaskContext ctx;
    protected final int numOfFields;
    protected final ArrayTupleBuilder tb;
    protected final FeedLogManager feedLogManager;
    protected boolean flushing;

    public AbstractFeedDataFlowController(IHyracksTaskContext ctx, FeedLogManager feedLogManager, int numOfFields) {
        this.feedLogManager = feedLogManager;
        this.numOfFields = numOfFields;
        this.ctx = ctx;
        this.tb = new ArrayTupleBuilder(numOfFields);
    }

    @Override
    public void flush() throws HyracksDataException {
        flushing = true;
        tupleForwarder.flush();
        flushing = false;
    }

    public abstract String getStats();

    @Override
    public void close() throws IOException {
        feedLogManager.close();
    }
}
