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

import java.util.Map;

import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.record.RecordWithPK;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class RecordWithPKTestReaderFactory implements IRecordReaderFactory<RecordWithPK<char[]>> {

    private static final long serialVersionUID = 1L;
    private transient AlgebricksAbsolutePartitionConstraint clusterLocations;

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException {
        clusterLocations = IExternalDataSourceFactory.getPartitionConstraints(clusterLocations, 1);
        return clusterLocations;
    }

    @Override
    public void configure(final Map<String, String> configuration) {
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
}
