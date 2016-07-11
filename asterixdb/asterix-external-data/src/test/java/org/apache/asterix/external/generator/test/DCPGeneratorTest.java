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
package org.apache.asterix.external.generator.test;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.RecordWithMetadataAndPK;
import org.apache.asterix.external.input.record.converter.DCPMessageToRecordConverter;
import org.apache.asterix.external.input.record.reader.kv.KVTestReader;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.junit.Test;

import com.couchbase.client.core.message.dcp.DCPRequest;

public class DCPGeneratorTest {

    @Test
    public void runTest() throws Exception {
        try (KVTestReader cbreader = new KVTestReader(0, "TestBucket",
                new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }, 150, 0, 0, 0)) {
            final UTF8StringPointable pointable = new UTF8StringPointable();
            final DCPMessageToRecordConverter converter = new DCPMessageToRecordConverter();
            while (cbreader.hasNext()) {
                final IRawRecord<DCPRequest> dcp = cbreader.next();
                final RecordWithMetadataAndPK<char[]> record = converter.convert(dcp);
                if (record.getRecord().size() == 0) {
                    pointable.set(record.getMetadata(0).getByteArray(), 1, record.getMetadata(0).getLength());
                } else {
                    pointable.set(record.getMetadata(0).getByteArray(), 1, record.getMetadata(0).getLength());
                }
            }
        } catch (final Throwable th) {
            System.err.println("TEST FAILED");
            th.printStackTrace();
            throw th;
        }
        System.err.println("TEST PASSED.");
    }
}
