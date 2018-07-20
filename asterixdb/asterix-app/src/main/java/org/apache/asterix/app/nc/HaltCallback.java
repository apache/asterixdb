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
package org.apache.asterix.app.nc;

import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HaltCallback implements IIoOperationFailedCallback {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final HaltCallback INSTANCE = new HaltCallback();

    private HaltCallback() {
    }

    @Override
    public void schedulerFailed(ILSMIOOperationScheduler scheduler, Throwable failure) {
        LOGGER.error("IO Scheduler has failed", failure);
        ExitUtil.halt(ExitUtil.EC_IO_SCHEDULER_FAILED);
    }

    @Override
    public void operationFailed(ILSMIOOperation operation, Throwable t) {
        LOGGER.error("Operation {} has failed", operation, t);
        if (operation.getIOOpertionType() == LSMIOOperationType.FLUSH) {
            ExitUtil.halt(ExitUtil.EC_FLUSH_FAILED);
        }
    }
}
