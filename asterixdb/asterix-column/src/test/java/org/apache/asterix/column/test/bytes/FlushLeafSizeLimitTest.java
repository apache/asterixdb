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
package org.apache.asterix.column.test.bytes;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.asterix.column.common.buffer.DummyBufferCache;
import org.apache.asterix.column.common.buffer.TestWriteMultiPageOp;
import org.apache.asterix.column.common.row.DummyLSMBTreeTupleReference;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleWriter;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.write.DefaultColumnWriteContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * The max leaf node size must bound every column, not only strings: a numeric array (e.g., an embedding) is stored
 * with a plain encoding and would otherwise grow a mega leaf node far beyond the configured size.
 */
public class FlushLeafSizeLimitTest {
    private static final int PAGE_SIZE = 4 * 1024;
    private static final int MAX_NUMBER_OF_TUPLES = 1000;
    private static final int MAX_LEAF_NODE_SIZE = 64 * 1024;
    private static final int EMBEDDING_DIMENSION = 384;

    @Test
    public void numericArrayBoundsLeafSize() throws Exception {
        DummyBufferCache bufferCache = new DummyBufferCache(PAGE_SIZE);
        int fileId = bufferCache.createFile();
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        IColumnValuesWriterFactory writerFactory = new ColumnValuesWriterFactory(multiPageOpRef);
        FlushColumnMetadata columnMetadata = new FlushColumnMetadata(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null,
                Collections.emptyList(), null, writerFactory, multiPageOpRef);
        columnMetadata.init(new TestWriteMultiPageOp(bufferCache, fileId));
        FlushColumnTupleWriter writer = new FlushColumnTupleWriter(columnMetadata, PAGE_SIZE, MAX_NUMBER_OF_TUPLES,
                0.15, MAX_LEAF_NODE_SIZE, DefaultColumnWriteContext.INSTANCE);

        ADMDataParser parser = new ADMDataParser(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, true);
        DummyLSMBTreeTupleReference tuple = new DummyLSMBTreeTupleReference();
        // 384 doubles are ~3KB per record, so the leaf must fill up within ~21 records rather than 1000
        int tupleCount = 0;
        try {
            while (writer.getMaxNumberOfTuples() > tupleCount) {
                ArrayBackedValueStorage record = new ArrayBackedValueStorage();
                parser.setInputStream(new ByteArrayInputStream(createRecord(tupleCount)));
                Assert.assertTrue(parser.parse(record.getDataOutput()));
                tuple.set(record);
                writer.writeTuple(tuple);
                tupleCount++;
            }
        } finally {
            writer.close();
        }

        int bytesPerRecord = EMBEDDING_DIMENSION * Double.BYTES;
        int expectedTupleCount = MAX_LEAF_NODE_SIZE / bytesPerRecord;
        Assert.assertTrue("Leaf size limit ignored numeric values, wrote " + tupleCount + " tuples",
                tupleCount <= expectedTupleCount + 1);
        Assert.assertTrue("Leaf size limit tripped too early, wrote " + tupleCount + " tuples",
                tupleCount >= expectedTupleCount / 2);
    }

    private static byte[] createRecord(int id) {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"id\": ").append(id).append(", \"emb\": [");
        for (int i = 0; i < EMBEDDING_DIMENSION; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(String.format("%.6f", Math.sin(id + i * 0.001)));
        }
        builder.append("]}");
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }
}
