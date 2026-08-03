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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.column.common.buffer.TestWriteMultiPageOp;
import org.apache.asterix.column.common.row.DummyLSMBTreeTupleReference;
import org.apache.asterix.column.common.test.TestCase;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleWithMetaWriter;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.write.DefaultColumnWriteContext;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Regression test for the page-zero under-sizing that caused {@code BufferOverflowException} on flush of
 * meta-bearing, meta-sparse collections (e.g. Sync Gateway xattrs).
 * <p>
 * The page-zero fit-check sizes the zeroth segment from {@code getAbsoluteNumberOfColumns(true)}
 * ({@code columnMetadataWithCurrentTuple}), while the flush lays it out from
 * {@code getAbsoluteNumberOfColumns(false)} ({@code columnMetadata}). For meta-bearing collections the real write
 * path grows {@code columnMetadata} with both the main record ({@code writeRecord}) and the meta record
 * ({@code writeMeta}), but the fit-check look-ahead ({@code updateColumnMetadataForCurrentTuple}) used to walk only
 * the main record. So the fit-check counted fewer columns than the flush wrote, under-sizing page zero.
 * <p>
 * This test drives {@link FlushColumnTupleWithMetaWriter} directly (the same record is used as both the main and the
 * meta record via {@link DummyLSMBTreeTupleReference}, so meta columns are materialized separately from the main
 * columns) and asserts the fit-check count equals the flush count. Before the fix the two diverge (fit-check counts
 * only the main columns); after the fix they match.
 */
@RunWith(Parameterized.class)
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class FlushMetaColumnCountTest extends AbstractBytesTest {

    public FlushMetaColumnCountTest(TestCase testCase) throws HyracksDataException {
        super(testCase);
    }

    @BeforeClass
    public static void setup() throws IOException {
        setup(FlushMetaColumnCountTest.class);
    }

    @Parameters(name = "MetaColumnCount {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return initTests(FlushMetaColumnCountTest.class, "small");
    }

    @Test
    public void fitCheckCountsMetaColumns() throws IOException {
        int fileId = createFile();

        // Meta-bearing metadata: metaType != null. numPrimaryKeys = 0 (empty primaryKeys) so no PK extraction is
        // needed from the dummy tuple; keySourceIndicator must still be non-null (it is dereferenced when
        // metaType != null).
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        IColumnValuesWriterFactory writerFactory = new ColumnValuesWriterFactory(multiPageOpRef);
        FlushColumnMetadata columnMetadata = new FlushColumnMetadata(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE,
                DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, Collections.emptyList(), Collections.singletonList(0),
                writerFactory, multiPageOpRef);
        columnMetadata.init(new TestWriteMultiPageOp(dummyBufferCache, fileId));

        FlushColumnTupleWithMetaWriter writer = new FlushColumnTupleWithMetaWriter(columnMetadata, PAGE_SIZE,
                MAX_NUMBER_OF_TUPLES, TOLERANCE, MAX_LEAF_NODE_SIZE, DefaultColumnWriteContext.INSTANCE);
        try {
            List<IValueReference> records = getParsedRecords();
            DummyLSMBTreeTupleReference tuple = new DummyLSMBTreeTupleReference();
            for (IValueReference record : records) {
                tuple.set(record);
                // Fit-check look-ahead (sizes page zero) then the real write (lays page zero out).
                writer.updateColumnMetadataForCurrentTuple(tuple);
                writer.writeTuple(tuple);
            }

            int flushColumns = writer.getAbsoluteNumberOfColumns(false); // what the flush lays out (main + meta)
            int fitCheckColumns = writer.getAbsoluteNumberOfColumns(true); // what the fit-check sizes for

            Assert.assertTrue("expected the meta-bearing records to produce columns", flushColumns > 0);
            Assert.assertEquals(
                    "page-zero fit-check must count the same columns the flush lays out (including meta columns)",
                    flushColumns, fitCheckColumns);
        } finally {
            writer.close();
        }
    }
}
