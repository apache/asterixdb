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
package org.apache.asterix.external.input.record.reader;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.record.RecordWithPK;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class RecordWithPKTestReaderFactory implements IRecordReaderFactory<RecordWithPK<char[]>> {

    private static final long serialVersionUID = 1L;
    private transient AlgebricksAbsolutePartitionConstraint clusterLocations;
    private transient IServiceContext serviceCtx;
    private static final List<String> recordReaderNames = Collections.unmodifiableList(Arrays.asList());

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException {
        clusterLocations = IExternalDataSourceFactory.getPartitionConstraints(
                (ICcApplicationContext) serviceCtx.getApplicationContext(), clusterLocations, 1);
        return clusterLocations;
    }

    @Override
    public void configure(IServiceContext serviceCtx, final Map<String, String> configuration) {
        this.serviceCtx = serviceCtx;
    }

    @Override
    public IRecordReader<? extends RecordWithPK<char[]>> createRecordReader(final IHyracksTaskContext ctx,
            final int partition) {
        return new TestAsterixMembersReader();
    }

    @Override
    public Class<?> getRecordClass() {
        return RecordWithPK.class;
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }
}
