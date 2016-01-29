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
package org.apache.asterix.external.input.record.reader.couchbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.asterix.external.api.IDataFlowController;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.input.record.RecordWithMetadata;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
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
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import rx.functions.Action1;

public class CouchbaseReader implements IRecordReader<RecordWithMetadata<char[]>> {

    private static final MutationMessage POISON_PILL = new MutationMessage((short) 0, null, null, 0, 0L, 0L, 0, 0, 0L,
            null);
    private final String feedName;
    private final short[] vbuckets;
    private final String bucket;
    private final String password;
    private final String[] couchbaseNodes;
    private AbstractFeedDataFlowController controller;
    private Builder builder;
    private BucketStreamAggregator bucketStreamAggregator;
    private CouchbaseCore core;
    private DefaultCoreEnvironment env;
    private Thread pushThread;
    private ArrayBlockingQueue<MutationMessage> messages;
    private GenericRecord<RecordWithMetadata<char[]>> record;
    private RecordWithMetadata<char[]> recordWithMetadata;
    private boolean done = false;
    private CharArrayRecord value;
    private CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private ByteBuffer bytes = ByteBuffer.allocateDirect(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
    private CharBuffer chars = CharBuffer.allocate(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
    // metaTypes = {key(string), bucket(string), vbucket(int32), seq(long), cas(long), creationTime(long),expiration(int32),flags(int32),revSeqNumber(long),lockTime(int32)}
    private static final IAType[] metaTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING,
            BuiltinType.AINT32, BuiltinType.AINT64, BuiltinType.AINT64, BuiltinType.AINT64, BuiltinType.AINT32,
            BuiltinType.AINT32, BuiltinType.AINT64, BuiltinType.AINT32 };
    private static final Logger LOGGER = Logger.getLogger(CouchbaseReader.class);

    public CouchbaseReader(String feedName, String bucket, String password, String[] couchbaseNodes, short[] vbuckets,
            int queueSize) throws HyracksDataException {
        this.feedName = feedName;
        this.bucket = bucket;
        this.password = password;
        this.couchbaseNodes = couchbaseNodes;
        this.vbuckets = vbuckets;
        this.recordWithMetadata = new RecordWithMetadata<char[]>(metaTypes, char[].class);
        this.messages = new ArrayBlockingQueue<MutationMessage>(queueSize);
        this.value = new CharArrayRecord();
        recordWithMetadata.setRecord(value);
        this.record = new GenericRecord<RecordWithMetadata<char[]>>(recordWithMetadata);
    }

    @Override
    public void close() {
        if (!done) {
            done = true;
        }
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        this.builder = DefaultCoreEnvironment.builder().dcpEnabled(CouchbaseReaderFactory.DCP_ENABLED)
                .autoreleaseAfter(CouchbaseReaderFactory.AUTO_RELEASE_AFTER_MILLISECONDS);
        this.env = builder.build();
        this.core = new CouchbaseCore(env);
        this.bucketStreamAggregator = new BucketStreamAggregator(feedName, core, bucket);
        connect();
    }

    private void connect() {
        core.send(new SeedNodesRequest(couchbaseNodes))
                .timeout(CouchbaseReaderFactory.TIMEOUT, CouchbaseReaderFactory.TIME_UNIT).toBlocking().single();
        core.send(new OpenBucketRequest(bucket, password))
                .timeout(CouchbaseReaderFactory.TIMEOUT, CouchbaseReaderFactory.TIME_UNIT).toBlocking().single();
        this.pushThread = new Thread(new Runnable() {
            @Override
            public void run() {
                CouchbaseReader.this.run(bucketStreamAggregator);
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
                public void call(final DCPRequest dcpRequest) {
                    try {
                        if (dcpRequest instanceof SnapshotMarkerMessage) {
                            SnapshotMarkerMessage message = (SnapshotMarkerMessage) dcpRequest;
                            final BucketStreamState oldState = state.get(message.partition());
                            state.put(new BucketStreamState(message.partition(), oldState.vbucketUUID(),
                                    message.endSequenceNumber(), oldState.endSequenceNumber(),
                                    message.endSequenceNumber(), oldState.snapshotEndSequenceNumber()));
                        } else if (dcpRequest instanceof MutationMessage) {

                            messages.put((MutationMessage) dcpRequest);
                        } else if (dcpRequest instanceof RemoveMessage) {
                            RemoveMessage message = (RemoveMessage) dcpRequest;
                            LOGGER.info(message.key() + " was deleted.");
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
                    CouchbaseReader.this.close();
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
    public IRawRecord<RecordWithMetadata<char[]>> next() throws IOException, InterruptedException {
        if (messages.isEmpty()) {
            controller.flush();
        }
        MutationMessage message = messages.take();
        if (message == POISON_PILL) {
            return null;
        }
        String key = message.key();
        int vbucket = message.partition();
        long seq = message.bySequenceNumber();
        String bucket = message.bucket();
        long cas = message.cas();
        long creationTime = message.creationTime();
        int expiration = message.expiration();
        int flags = message.flags();
        long revSeqNumber = message.revisionSequenceNumber();
        int lockTime = message.lockTime();
        recordWithMetadata.reset();
        recordWithMetadata.setMetadata(0, key);
        recordWithMetadata.setMetadata(1, bucket);
        recordWithMetadata.setMetadata(2, vbucket);
        recordWithMetadata.setMetadata(3, seq);
        recordWithMetadata.setMetadata(4, cas);
        recordWithMetadata.setMetadata(5, creationTime);
        recordWithMetadata.setMetadata(6, expiration);
        recordWithMetadata.setMetadata(7, flags);
        recordWithMetadata.setMetadata(8, revSeqNumber);
        recordWithMetadata.setMetadata(9, lockTime);
        CouchbaseReader.set(message.content(), decoder, bytes, chars, value);
        return record;
    }

    @Override
    public boolean stop() {
        done = true;
        core.send(new CloseBucketRequest(bucket)).toBlocking();
        try {
            messages.put(CouchbaseReader.POISON_PILL);
        } catch (InterruptedException e) {
            LOGGER.warn(e);
            return false;
        }
        return true;
    }

    @Override
    public void setController(IDataFlowController controller) {
        this.controller = (AbstractFeedDataFlowController) controller;
    }

    public static void set(ByteBuf content, CharsetDecoder decoder, ByteBuffer bytes, CharBuffer chars,
            CharArrayRecord record) {
        int position = content.readerIndex();
        int limit = content.writerIndex();
        int contentSize = content.capacity();
        while (position < limit) {
            bytes.clear();
            chars.clear();
            if (contentSize - position < bytes.capacity()) {
                bytes.limit(contentSize - position);
            }
            content.getBytes(position, bytes);
            position += bytes.position();
            bytes.flip();
            decoder.decode(bytes, chars, false);
            chars.flip();
            record.append(chars);
        }
        record.endRecord();
    }
}
