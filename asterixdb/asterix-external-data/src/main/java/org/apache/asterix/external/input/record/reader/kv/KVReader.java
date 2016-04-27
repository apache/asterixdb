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
import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.message.cluster.CloseBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;

import rx.Subscriber;

public class KVReader implements IRecordReader<DCPRequest> {

    private static final Logger LOGGER = Logger.getLogger(KVReader.class);
    private static final MutationMessage POISON_PILL = new MutationMessage(0, (short) 0, null, null, 0, 0L, 0L, 0, 0,
            0L, null);
    private final String feedName;
    private final short[] vbuckets;
    private final String bucket;
    private final String password;
    private final String[] sourceNodes;
    private final CouchbaseCore core;
    private final GenericRecord<DCPRequest> record;
    private final ArrayBlockingQueue<DCPRequest> messages;
    private AbstractFeedDataFlowController controller;
    private Thread pushThread;
    private boolean done = false;

    public KVReader(String feedName, String bucket, String password, String[] sourceNodes, short[] vbuckets,
            int queueSize, CouchbaseCore core) throws HyracksDataException {
        this.feedName = feedName;
        this.bucket = bucket;
        this.password = password;
        this.sourceNodes = sourceNodes;
        this.vbuckets = vbuckets;
        this.messages = new ArrayBlockingQueue<DCPRequest>(queueSize);
        this.core = core;
        this.record = new GenericRecord<>();
        this.pushThread = new Thread(new Runnable() {
            @Override
            public void run() {
                KVReader.this.run();
            }
        }, feedName);
        pushThread.start();
    }

    @Override
    public void close() {
        if (!done) {
            done = true;
        }
    }

    private void run() {
        core.send(new SeedNodesRequest(sourceNodes)).timeout(KVReaderFactory.TIMEOUT, KVReaderFactory.TIME_UNIT)
                .toBlocking().single();
        core.send(new OpenBucketRequest(bucket, password)).timeout(KVReaderFactory.TIMEOUT, KVReaderFactory.TIME_UNIT)
                .toBlocking().single();
        DCPConnection connection = core.<OpenConnectionResponse> send(new OpenConnectionRequest(feedName, bucket))
                .toBlocking().single().connection();
        for (int i = 0; i < vbuckets.length; i++) {
            connection.addStream(vbuckets[i]).toBlocking().single();
        }
        try {
            connection.subject().toBlocking().subscribe(new Subscriber<DCPRequest>() {
                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public void onNext(DCPRequest dcpRequest) {
                    try {
                        if (dcpRequest instanceof SnapshotMarkerMessage) {
                            SnapshotMarkerMessage message = (SnapshotMarkerMessage) dcpRequest;
                            LOGGER.info("snapshot DCP message received: " + message);
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
            th.printStackTrace();
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
        pushThread.interrupt();
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
