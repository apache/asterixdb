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

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment.Builder;
import com.couchbase.client.core.message.cluster.CloseBucketRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;

import rx.functions.Func1;

public class KVReaderFactory implements IRecordReaderFactory<DCPRequest> {

    private static final long serialVersionUID = 1L;
    // Constant fields
    public static final boolean DCP_ENABLED = true;
    public static final long AUTO_RELEASE_AFTER_MILLISECONDS = 5000L;
    public static final int TIMEOUT = 5;
    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
    // Dynamic fields
    private Map<String, String> configuration;
    private String bucket;
    private String password = "";
    private String[] couchbaseNodes;
    private int numOfVBuckets;
    private int[] schedule;
    private String feedName;
    // Transient fields
    private transient CouchbaseCore core;
    private transient Builder builder;
    private transient DefaultCoreEnvironment env;

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return AsterixClusterProperties.INSTANCE.getClusterLocations();
    }

    @Override
    public void configure(Map<String, String> configuration) throws AsterixException {
        // validate first
        if (!configuration.containsKey(ExternalDataConstants.KEY_BUCKET)) {
            throw new AsterixException("Unspecified bucket");
        }
        if (!configuration.containsKey(ExternalDataConstants.KEY_NODES)) {
            throw new AsterixException("Unspecified Couchbase nodes");
        }
        if (configuration.containsKey(ExternalDataConstants.KEY_PASSWORD)) {
            password = configuration.get(ExternalDataConstants.KEY_PASSWORD);
        }
        this.configuration = configuration;
        ExternalDataUtils.setNumberOfKeys(configuration, 1);
        ExternalDataUtils.setChangeFeed(configuration, ExternalDataConstants.TRUE);
        ExternalDataUtils.setRecordWithMeta(configuration, ExternalDataConstants.TRUE);
        bucket = configuration.get(ExternalDataConstants.KEY_BUCKET);
        couchbaseNodes = configuration.get(ExternalDataConstants.KEY_NODES).split(",");
        feedName = configuration.get(ExternalDataConstants.KEY_FEED_NAME);
        builder = DefaultCoreEnvironment.builder().dcpEnabled(DCP_ENABLED)
                .autoreleaseAfter(AUTO_RELEASE_AFTER_MILLISECONDS);
        env = builder.build();
        core = new CouchbaseCore(env);
        getNumberOfVbuckets();
        schedule();
    }

    /*
     * We distribute the work of streaming vbuckets between all the partitions in a round robin
     * fashion.
     */
    private void schedule() {
        schedule = new int[numOfVBuckets];
        String[] locations = AsterixClusterProperties.INSTANCE.getClusterLocations().getLocations();
        for (int i = 0; i < numOfVBuckets; i++) {
            schedule[i] = i % locations.length;
        }
    }

    private void getNumberOfVbuckets() {
        core.send(new SeedNodesRequest(couchbaseNodes)).timeout(TIMEOUT, TIME_UNIT).toBlocking().single();
        core.send(new OpenBucketRequest(bucket, password)).timeout(TIMEOUT, TIME_UNIT).toBlocking().single();
        numOfVBuckets = core.<GetClusterConfigResponse> send(new GetClusterConfigRequest())
                .map(new Func1<GetClusterConfigResponse, Integer>() {
                    @Override
                    public Integer call(GetClusterConfigResponse response) {
                        CouchbaseBucketConfig config = (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                        return config.numberOfPartitions();
                    }
                }).timeout(TIMEOUT, TIME_UNIT).toBlocking().single();
        core.send(new CloseBucketRequest(bucket)).toBlocking();
    }

    @Override
    public IRecordReader<? extends DCPRequest> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        String nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();
        ArrayList<Short> listOfAssignedVBuckets = new ArrayList<Short>();
        for (int i = 0; i < schedule.length; i++) {
            if (schedule[i] == partition) {
                listOfAssignedVBuckets.add((short) i);
            }
        }
        short[] vbuckets = new short[listOfAssignedVBuckets.size()];
        for (int i = 0; i < vbuckets.length; i++) {
            vbuckets[i] = listOfAssignedVBuckets.get(i);
        }
        return new KVReader(feedName + ":" + nodeName + ":" + partition, bucket, password, couchbaseNodes,
                vbuckets, ExternalDataUtils.getQueueSize(configuration));
    }

    @Override
    public Class<?> getRecordClass() {
        return DCPRequest.class;
    }
}
