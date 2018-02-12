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

package org.apache.hyracks.storage.am.common.dataflow;

import static org.apache.hyracks.api.exceptions.ErrorCode.CANNOT_DROP_IN_USE_INDEX;
import static org.apache.hyracks.api.exceptions.ErrorCode.INDEX_DOES_NOT_EXIST;
import static org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption.IF_EXISTS;
import static org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption.WAIT_ON_IN_USE;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexDropOperatorNodePushable extends AbstractOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long DROP_ATTEMPT_WAIT_TIME_MILLIS = TimeUnit.SECONDS.toMillis(1);
    private final IIndexDataflowHelper indexHelper;
    private final Set<DropOption> options;
    private long maxWaitTimeMillis = TimeUnit.SECONDS.toMillis(30);

    public IndexDropOperatorNodePushable(IIndexDataflowHelperFactory indexHelperFactory, Set<DropOption> options,
            IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        this.indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
        this.options = options;
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        // no op
    }

    @Override
    public int getInputArity() {
        return 0;
    }

    @Override
    public IFrameWriter getInputFrameWriter(int index) {
        return null;
    }

    @Override
    public void initialize() throws HyracksDataException {
        dropIndex();
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        // no op
    }

    private void dropIndex() throws HyracksDataException {
        while (true) {
            try {
                indexHelper.destroy();
                return;
            } catch (HyracksDataException e) {
                if (isIgnorable(e)) {
                    LOGGER.debug("Ignoring exception on drop", e);
                    return;
                }
                if (canRetry(e)) {
                    LOGGER.info("Retrying drop on exception", e);
                    continue;
                }
                throw e;
            }
        }
    }

    private boolean isIgnorable(HyracksDataException e) {
        return e.getErrorCode() == INDEX_DOES_NOT_EXIST && options.contains(IF_EXISTS);
    }

    private boolean canRetry(HyracksDataException e) throws HyracksDataException {
        if (e.getErrorCode() == CANNOT_DROP_IN_USE_INDEX && options.contains(WAIT_ON_IN_USE)) {
            if (maxWaitTimeMillis <= 0) {
                return false;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(DROP_ATTEMPT_WAIT_TIME_MILLIS);
                maxWaitTimeMillis -= DROP_ATTEMPT_WAIT_TIME_MILLIS;
                return true;
            } catch (InterruptedException e1) {
                throw HyracksDataException.create(e1);
            }
        }
        return false;
    }
}
