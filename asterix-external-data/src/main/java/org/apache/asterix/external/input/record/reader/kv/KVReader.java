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

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.log4j.Logger;

import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.dcp.BucketStreamAggregator;
import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.client.core.dcp.BucketStreamStateUpdatedEvent;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment.Builder;
import com.couchbase.client.core.message.cluster.CloseBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;

import rx.functions.Action1;

public class KVReader implements IRecordReader<DCPRequest> {

    private static final Logger LOGGER = Logger.getLogger(KVReader.class);
    private static final MutationMessage POISON_PILL = new MutationMessage((short) 0, null, null, 0, 0L, 0L, 0, 0, 0L,
            null);
    private final String feedName;
    private final short[] vbuckets;
    private final String bucket;
    private final String password;
    private final String[] sourceNodes;
    private final Builder builder;
    private final BucketStreamAggregator bucketStreamAggregator;
    private final CouchbaseCore core;
    private final DefaultCoreEnvironment env;
    private final GenericRecord<DCPRequest> record;
    private final ArrayBlockingQueue<DCPRequest> messages;
    private AbstractFeedDataFlowController controller;
    private Thread pushThread;
    private boolean done = false;

    public KVReader(String feedName, String bucket, String password, String[] sourceNodes, short[] vbuckets,
            int queueSize) throws HyracksDataException {
        this.feedName = feedName;
        this.bucket = bucket;
        this.password = password;
        this.sourceNodes = sourceNodes;
        this.vbuckets = vbuckets;
        this.messages = new ArrayBlockingQueue<DCPRequest>(queueSize);
        this.builder = DefaultCoreEnvironment.builder().dcpEnabled(KVReaderFactory.DCP_ENABLED)
                .autoreleaseAfter(KVReaderFactory.AUTO_RELEASE_AFTER_MILLISECONDS);
        this.env = builder.build();
        this.core = new CouchbaseCore(env);
        this.bucketStreamAggregator = new BucketStreamAggregator(feedName, core, bucket);
        this.record = new GenericRecord<>();
        connect();
    }

    @Override
    public void close() {
        if (!done) {
            done = true;
        }
    }

    private void connect() {
        core.send(new SeedNodesRequest(sourceNodes)).timeout(KVReaderFactory.TIMEOUT, KVReaderFactory.TIME_UNIT)
                .toBlocking().single();
        core.send(new OpenBucketRequest(bucket, password)).timeout(KVReaderFactory.TIMEOUT, KVReaderFactory.TIME_UNIT)
                .toBlocking().single();
        this.pushThread = new Thread(new Runnable() {
            @Override
            public void run() {
                KVReader.this.run(bucketStreamAggregator);
            }
        }, feedName);
        pushThread.start();
    }

    private void run(BucketStreamAggregator bucketStreamAggregator) {
        BucketStreamAggregatorState state = new BucketStreamAggregatorState();
        for (int i = 0; i < vbuckets.length; i++) {
            state.put(new BucketStreamState(vbuckets[i], 0, 0, 0xffffffff, 0, 0xffffffff));
        }
        state.updates().subscribe(new Action1<BucketStreamStateUpdatedEvent>() {
            @Override
            public void call(BucketStreamStateUpdatedEvent event) {
                if (event.partialUpdate()) {
                } else {
                }
            }
        });
        try {
            bucketStreamAggregator.feed(state).toBlocking().forEach(new Action1<DCPRequest>() {
                @Override
                public void call(DCPRequest dcpRequest) {
                    try {
                        if (dcpRequest instanceof SnapshotMarkerMessage) {
                            SnapshotMarkerMessage message = (SnapshotMarkerMessage) dcpRequest;
                            BucketStreamState oldState = state.get(message.partition());
                            state.put(new BucketStreamState(message.partition(), oldState.vbucketUUID(),
                                    message.endSequenceNumber(), oldState.endSequenceNumber(),
                                    message.endSequenceNumber(), oldState.snapshotEndSequenceNumber()));
                        } else if ((dcpRequest instanceof MutationMessage) || (dcpRequest instanceof RemoveMessage)) {
                            messages.put(dcpRequest);
                        } else {
                            LOGGER.warn("Unknown type of DCP messages: " + dcpRequest);
                        }
                    } catch (Throwable th) {
                        LOGGER.error(th);
                    }
                }
            });
        } catch (Throwable th) {
            if (th.getCause() instanceof InterruptedException) {
                LOGGER.warn("dcp thread was interrupted", th);
                synchronized (this) {
                    KVReader.this.close();
                    notifyAll();
                }
            }
            throw th;
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        return !done;
    }

    @Override
    public IRawRecord<DCPRequest> next() throws IOException, InterruptedException {
        if (messages.isEmpty()) {
            controller.flush();
        }
        DCPRequest dcpRequest = messages.take();
        if (dcpRequest == POISON_PILL) {
            return null;
        }
        record.set(dcpRequest);
        return record;
    }

    @Override
    public boolean stop() {
        done = true;
        core.send(new CloseBucketRequest(bucket)).toBlocking();
        try {
            messages.put(KVReader.POISON_PILL);
        } catch (InterruptedException e) {
            LOGGER.warn(e);
            return false;
        }
        return true;
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        this.controller = controller;
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) {
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }
}
