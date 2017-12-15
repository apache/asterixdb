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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.ByteBufAllocator;

public class KVTestReader implements IRecordReader<DCPRequest> {

    private final GenericRecord<DCPRequest> record;
    private static final Logger LOGGER = LogManager.getLogger();
    // Test variables
    private final String bucket;
    private final ArrayList<Short> assigned;
    private final int numberOfMutations;
    private int counter = 0;
    private int upsertCounter = 0;
    private boolean stopped = false;
    // for deterministic data generation
    private int expiration = 7999;
    private long seq = 16L;
    private int lockTime = 158;
    private long cas = 0L;
    private int deleteCycle;
    private int upsertCycle;
    private String nextDeleteKey;
    private short nextDeletePartition;
    private String nextUpsertKey;
    private short nextUpsertPartition;
    private final ByteBuf byteBuff;
    private final StringBuilder strBuilder = new StringBuilder();
    private final String[] names = { "Michael Carey", "Till Westmann", "Michael Blow", "Chris Hillary", "Yingyi Bu",
            "Ian Maxon", "Abdullah Alamoudi" };

    public KVTestReader(final int partition, final String bucket, final int[] schedule, final int numberOfMutations,
            final int deleteCycle, final int upsertCycle, int counterStart) {
        this.bucket = bucket;
        this.numberOfMutations = numberOfMutations + counterStart;
        this.assigned = new ArrayList<>();
        this.deleteCycle = deleteCycle;
        this.upsertCycle = upsertCycle;
        if ((deleteCycle < 5) || (upsertCycle < 5)) {
            this.deleteCycle = 5;
            this.upsertCycle = 6;
        }
        for (int i = 0; i < schedule.length; i++) {
            if (schedule[i] == partition) {
                assigned.add((short) i);
            }
        }
        this.byteBuff = ByteBufAllocator.DEFAULT.buffer(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
        this.record = new GenericRecord<DCPRequest>();
        this.counter = counterStart;
    }

    private String generateKey() {
        final short vbucket = assigned.get(counter % assigned.size());
        final String next = vbucket + "-" + counter;
        counter++;
        if ((counter % deleteCycle) == 0) {
            nextDeleteKey = next;
            nextDeletePartition = vbucket;
        }
        if ((counter % upsertCycle) == 3) {
            nextUpsertKey = next;
            nextUpsertPartition = vbucket;
        }
        return next;
    }

    @Override
    public void close() throws IOException {
        stop();
    }

    @Override
    public boolean hasNext() throws Exception {
        return !stopped;
    }

    @Override
    public IRawRecord<DCPRequest> next() throws IOException, InterruptedException {
        if (stopped) {
            return null;
        }
        try {
            final DCPRequest dcpRequest = generateNextDCPMessage();
            record.set(dcpRequest);
            if (counter >= numberOfMutations) {
                stop();
            }
        } catch (final Throwable th) {
            LOGGER.error(th.getMessage(), th);
        }
        return record;
    }

    private DCPRequest generateNextDCPMessage() {
        if ((counter % deleteCycle) == (deleteCycle - 1)) {
            if (nextDeleteKey != null) {
                final String key = nextDeleteKey;
                nextDeleteKey = null;
                return new RemoveMessage(0, nextDeletePartition, key, cas++, seq++, 0L, bucket);
            }
        }
        generateNextDocument();
        if ((counter % upsertCycle) == (upsertCycle - 1)) {
            if (nextUpsertKey != null) {
                final String key = nextUpsertKey;
                nextUpsertKey = null;
                upsertCounter++;
                return new MutationMessage(byteBuff.readableBytes(), nextUpsertPartition, key, byteBuff, expiration++,
                        seq++, 0, 0, lockTime++, cas++, bucket);
            }
        }
        return new MutationMessage(byteBuff.readableBytes(), assigned.get(counter % assigned.size()), generateKey(),
                byteBuff, expiration++, seq++, 0, 0, lockTime++, cas++, bucket);
    }

    private void generateNextDocument() {
        byteBuff.retain();
        // reset the string
        strBuilder.setLength(0);
        strBuilder.append("{\"id\":" + (counter + upsertCounter) + ",\"name\":\""
                + names[(counter + upsertCounter) % names.length] + "\"");
        switch (counter % 3) {
            case 0:
                // Missing
                break;
            case 1:
                strBuilder.append(",\"exp\":null");
                break;
            case 2:
                strBuilder.append(",\"exp\":" + ((counter + upsertCounter) * 3));
                break;
            default:
                break;
        }
        strBuilder.append("}");
        byteBuff.clear();
        byteBuff.writeBytes(strBuilder.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean stop() {
        if (!stopped) {
            stopped = true;
            byteBuff.release();
        }
        return stopped;
    }

    @Override
    public void setController(final AbstractFeedDataFlowController controller) {
    }

    @Override
    public void setFeedLogManager(final FeedLogManager feedLogManager) {
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

}
