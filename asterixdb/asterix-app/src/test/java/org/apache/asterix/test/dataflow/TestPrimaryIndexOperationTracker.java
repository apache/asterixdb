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
package org.apache.asterix.test.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.impl.ITestOpCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;

public class TestPrimaryIndexOperationTracker extends PrimaryIndexOperationTracker {

    private final List<ITestOpCallback<Void>> callbacks = new ArrayList<>();

    public TestPrimaryIndexOperationTracker(int datasetID, int partition, ILogManager logManager, DatasetInfo dsInfo,
            ILSMComponentIdGenerator idGenerator) {
        super(datasetID, partition, logManager, dsInfo, idGenerator);
    }

    public void addCallback(ITestOpCallback<Void> callback) {
        synchronized (callbacks) {
            callbacks.add(callback);
        }
    }

    public void clearCallbacks() {
        synchronized (callbacks) {
            callbacks.clear();
        }
    }

    @Override
    public void triggerScheduleFlush(LogRecord logRecord) throws HyracksDataException {
        synchronized (callbacks) {
            for (ITestOpCallback<Void> callback : callbacks) {
                callback.before(null);
            }
        }
        super.triggerScheduleFlush(logRecord);
        synchronized (callbacks) {
            for (ITestOpCallback<Void> callback : callbacks) {
                callback.after(null);
            }
        }
    }

}
