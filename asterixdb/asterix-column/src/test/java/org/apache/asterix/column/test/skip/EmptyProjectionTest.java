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
package org.apache.asterix.column.test.skip;

import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.common.buffer.DummyPage;
import org.apache.asterix.column.common.test.TestCase;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.query.ColumnAssembler;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.test.bytes.AbstractBytesTest;
import org.apache.asterix.column.test.bytes.components.TestColumnBufferProvider;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * An empty projection (e.g., {@code COUNT(*)}) returns the cached empty record; it must be the record the regular
 * assembly path produces, once per tuple, and stay in step with skips.
 */
@RunWith(Parameterized.class)
public class EmptyProjectionTest extends AbstractBytesTest {
    private static final int REPEAT = 3;

    public EmptyProjectionTest(TestCase testCase) throws HyracksDataException {
        super(testCase);
    }

    @Parameters(name = "EmptyProjectionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        List<Object[]> cases = new ArrayList<>();
        for (File file : listFiles(DATA_PATH)) {
            cases.add(new Object[] { new TestCase(file, file, OUTPUT_PATH) });
        }
        return cases;
    }

    @Test
    public void emptyProjectionYieldsOneEmptyRecordPerTuple() throws IOException {
        int fileId = createFile();
        FlushColumnMetadata columnMetadata = prepareNewFile(fileId);
        List<IValueReference> records = getParsedRecords();
        List<DummyPage> pageZeros = transform(fileId, columnMetadata, records, records.size() * REPEAT);

        ColumnAssembler full = createAssembler(columnMetadata, ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE);
        ColumnAssembler empty = createAssembler(columnMetadata, ProjectionFiltrationTypeUtil.EMPTY_TYPE);
        Assert.assertEquals(0, empty.getNumberOfColumns());
        TestColumnBufferProvider[] providers = createProviders(fileId, full);
        MultiByteBufferInputStream[] streams = createStreams(full);

        int pageNumber = 0;
        for (DummyPage pageZero : pageZeros) {
            String where = "page " + pageNumber;
            // the regular path builds this record once the page is exhausted
            int tupleCount = reset(pageZero, full, providers, streams);
            full.skip(tupleCount);
            byte[] expected = copy(full.nextValue());

            empty.reset(tupleCount);
            int assembled = 0;
            while (empty.hasNext()) {
                Assert.assertArrayEquals(where, expected, copy(empty.nextValue()));
                assembled++;
            }
            Assert.assertEquals(where, tupleCount, assembled);
            Assert.assertArrayEquals(where + " past the end", expected, copy(empty.nextValue()));
            Assert.assertFalse(where, empty.hasNext());

            // skips and reads must still account for every tuple exactly once
            empty.reset(tupleCount);
            int read = 0;
            while (empty.hasNext()) {
                empty.nextValue();
                read++;
                if (empty.hasNext()) {
                    empty.skip(1);
                    read++;
                }
            }
            Assert.assertEquals(where + " with skips", tupleCount, read);
            pageNumber++;
        }
    }

    private static ColumnAssembler createAssembler(FlushColumnMetadata columnMetadata, ARecordType requestedType)
            throws IOException {
        return QueryColumnMetadata.create(columnMetadata.getDatasetType(), columnMetadata.getNumberOfPrimaryKeys(),
                columnMetadata.serializeColumnsMetadata(), new ColumnValueReaderFactory(), ValueGetterFactory.INSTANCE,
                requestedType, Collections.emptyMap(), NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpWarningCollector.INSTANCE, null,
                ColumnProjectorType.QUERY).getAssembler();
    }

    private TestColumnBufferProvider[] createProviders(int fileId, ColumnAssembler assembler) {
        TestColumnBufferProvider[] providers = new TestColumnBufferProvider[assembler.getNumberOfColumns()];
        for (int i = 0; i < providers.length; i++) {
            providers[i] = new TestColumnBufferProvider(fileId, assembler.getColumnIndex(i), dummyBufferCache);
        }
        return providers;
    }

    private static MultiByteBufferInputStream[] createStreams(ColumnAssembler assembler) {
        MultiByteBufferInputStream[] streams = new MultiByteBufferInputStream[assembler.getNumberOfColumns()];
        for (int i = 0; i < streams.length; i++) {
            streams[i] = new MultiByteBufferInputStream();
        }
        return streams;
    }

    private static int reset(DummyPage pageZero, ColumnAssembler assembler, TestColumnBufferProvider[] providers,
            MultiByteBufferInputStream[] streams) throws HyracksDataException {
        for (int i = 0; i < providers.length; i++) {
            providers[i].reset(pageZero);
            streams[i].reset(providers[i]);
        }
        int tupleCount = pageZero.getBuffer().getInt(TUPLE_COUNT_OFFSET);
        assembler.reset(tupleCount);
        for (int i = 0; i < streams.length; i++) {
            assembler.resetColumn(streams[i], i);
        }
        return tupleCount;
    }

    private static byte[] copy(IValueReference value) {
        byte[] bytes = new byte[value.getLength()];
        System.arraycopy(value.getByteArray(), value.getStartOffset(), bytes, 0, bytes.length);
        return bytes;
    }
}
