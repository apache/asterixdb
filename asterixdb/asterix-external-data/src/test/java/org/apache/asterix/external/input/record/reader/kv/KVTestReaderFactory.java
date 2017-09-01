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
package org.apache.asterix.external.input.record.reader.kv;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;

import com.couchbase.client.core.message.dcp.DCPRequest;

public class KVTestReaderFactory implements IRecordReaderFactory<DCPRequest> {

    private static final long serialVersionUID = 1L;
    private final String bucket = "TestBucket";
    private final int numOfVBuckets = 1024;
    private final int[] schedule = new int[numOfVBuckets];
    private int numOfRecords = 1000; // default = 1 Million
    private int deleteCycle = 0;
    private int upsertCycle = 0;
    private int numOfReaders;
    private transient AlgebricksAbsolutePartitionConstraint clusterLocations;
    private transient IServiceContext serviceCtx;
    private static final List<String> recordReaderNames = Collections.unmodifiableList(Arrays.asList());

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        if (clusterLocations == null) {
            ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
            clusterLocations = appCtx.getClusterStateManager().getClusterLocations();
            numOfReaders = clusterLocations.getLocations().length;
        }
        return clusterLocations;

    }

    @Override
    public void configure(IServiceContext serviceCtx, final Map<String, String> configuration) {
        this.serviceCtx = serviceCtx;
        if (configuration.containsKey("num-of-records")) {
            numOfRecords = Integer.parseInt(configuration.get("num-of-records"));
        }
        final int numOfReaders = getPartitionConstraint().getLocations().length;
        for (int i = 0; i < numOfVBuckets; i++) {
            schedule[i] = i % numOfReaders;
        }

        if (configuration.containsKey("delete-cycle")) {
            deleteCycle = Integer.parseInt(configuration.get("delete-cycle"));
        }

        if (configuration.containsKey("upsert-cycle")) {
            upsertCycle = Integer.parseInt(configuration.get("upsert-cycle"));
        }
    }

    @Override
    public IRecordReader<? extends DCPRequest> createRecordReader(final IHyracksTaskContext ctx, final int partition) {
        return new KVTestReader(partition, bucket, schedule,
                (int) Math.ceil((double) numOfRecords / (double) numOfReaders), deleteCycle, upsertCycle,
                (numOfRecords / numOfReaders) * partition);
    }

    @Override
    public Class<?> getRecordClass() {
        return DCPRequest.class;
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }
}
