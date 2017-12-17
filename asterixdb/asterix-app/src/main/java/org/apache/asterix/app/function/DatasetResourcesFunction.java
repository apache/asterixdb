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
package org.apache.asterix.app.function;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.DatasetResource;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.metadata.declared.AbstractDatasourceFunction;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class DatasetResourcesFunction extends AbstractDatasourceFunction {

    private static final long serialVersionUID = 1L;
    private final int datasetId;

    public DatasetResourcesFunction(AlgebricksAbsolutePartitionConstraint locations, int datasetId) {
        super(locations);
        this.datasetId = datasetId;
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition) {
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        INcApplicationContext appCtx = (INcApplicationContext) serviceCtx.getApplicationContext();
        DatasetLifecycleManager dsLifecycleMgr = (DatasetLifecycleManager) appCtx.getDatasetLifecycleManager();
        DatasetResource dsr = dsLifecycleMgr.getDatasetLifecycle(datasetId);
        return new DatasetResourcesReader(dsr);
    }

}
