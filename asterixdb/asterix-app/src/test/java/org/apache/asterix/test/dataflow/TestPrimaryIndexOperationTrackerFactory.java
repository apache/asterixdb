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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.DatasetResource;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerFactory;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;

public class TestPrimaryIndexOperationTrackerFactory extends PrimaryIndexOperationTrackerFactory {

    private static final long serialVersionUID = 1L;
    private int datasetId;

    public TestPrimaryIndexOperationTrackerFactory(int datasetId) {
        super(datasetId);
        this.datasetId = datasetId;
    }

    @Override
    public ILSMOperationTracker getOperationTracker(INCServiceContext ctx) {
        try {
            INcApplicationContext appCtx = (INcApplicationContext) ctx.getApplicationContext();
            DatasetLifecycleManager dslcManager = (DatasetLifecycleManager) appCtx.getDatasetLifecycleManager();
            DatasetResource dsr = dslcManager.getDatasetLifecycle(datasetId);
            PrimaryIndexOperationTracker opTracker = dsr.getOpTracker();
            if (!(opTracker instanceof TestPrimaryIndexOperationTracker)) {
                Field opTrackerField = DatasetResource.class.getDeclaredField("datasetPrimaryOpTracker");
                opTracker = new TestPrimaryIndexOperationTracker(datasetId,
                        appCtx.getTransactionSubsystem().getLogManager(), dsr.getDatasetInfo(), dsr.getIdGenerator());
                setFinal(opTrackerField, dsr, opTracker);
            }
            return opTracker;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void setFinal(Field field, Object obj, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(obj, newValue);
    }
}
