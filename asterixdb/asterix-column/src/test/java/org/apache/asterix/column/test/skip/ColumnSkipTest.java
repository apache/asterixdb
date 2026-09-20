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
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

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
 * Positioning a mega leaf's columns at tuple k by skipping must yield the same tuple as reading the k tuples before
 * it. Every corpus in the data directory goes through the real flush path, so the readers see the level streams the
 * writer really produces: arrays of one type, arrays mixing types (one column per type, missing entries for the
 * others), null and missing items, empty arrays and nested arrays. A synthetic corpus adds embedding-sized arrays of
 * doubles with integer and null components, the case that motivated the bulk skip.
 */
@RunWith(Parameterized.class)
public class ColumnSkipTest extends AbstractBytesTest {
    private static final int REPEAT = 5;
    private static final int EMBEDDING_DIMENSIONS = 384;
    private static final int EMBEDDING_RECORDS = 90;

    public ColumnSkipTest(TestCase testCase) throws HyracksDataException {
        super(testCase);
    }

    @Parameters(name = "SkipTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        List<Object[]> cases = new ArrayList<>();
        for (File file : listFiles(DATA_PATH)) {
            cases.add(new Object[] { new TestCase(file, file, OUTPUT_PATH) });
        }
        File embeddings = writeEmbeddingCorpus();
        cases.add(new Object[] { new TestCase(embeddings, embeddings, OUTPUT_PATH) });
        return cases;
    }

    @Test
    public void skipLandsOnTheSameTupleAsSequentialReads() throws IOException {
        int fileId = createFile();
        FlushColumnMetadata columnMetadata = prepareNewFile(fileId);
        List<IValueReference> records = getParsedRecords();
        List<DummyPage> pageZeros = transform(fileId, columnMetadata, records, records.size() * REPEAT);
        QueryColumnMetadata readMetadata = QueryColumnMetadata.create(columnMetadata.getDatasetType(),
                columnMetadata.getNumberOfPrimaryKeys(), columnMetadata.serializeColumnsMetadata(),
                new ColumnValueReaderFactory(), ValueGetterFactory.INSTANCE,
                ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE, Collections.emptyMap(),
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpWarningCollector.INSTANCE, null, ColumnProjectorType.QUERY);
        ColumnAssembler assembler = readMetadata.getAssembler();
        int numberOfColumns = assembler.getNumberOfColumns();
        TestColumnBufferProvider[] providers = new TestColumnBufferProvider[numberOfColumns];
        MultiByteBufferInputStream[] streams = new MultiByteBufferInputStream[numberOfColumns];
        for (int i = 0; i < numberOfColumns; i++) {
            providers[i] = new TestColumnBufferProvider(fileId, assembler.getColumnIndex(i), dummyBufferCache);
            streams[i] = new MultiByteBufferInputStream();
        }

        Random random = new Random(20260920);
        int pageNumber = 0;
        for (DummyPage pageZero : pageZeros) {
            int tupleCount = reset(pageZero, assembler, providers, streams);
            List<byte[]> expected = new ArrayList<>(tupleCount);
            while (assembler.hasNext()) {
                expected.add(copy(assembler.nextValue()));
            }
            Assert.assertEquals("page " + pageNumber, tupleCount, expected.size());

            for (int k = 0; k < tupleCount; k++) {
                String where = "page " + pageNumber + " tuple " + k;
                // one skip straight to k, the way a point lookup positions the cursor
                reset(pageZero, assembler, providers, streams);
                assembler.skip(k);
                Assert.assertArrayEquals(where, expected.get(k), copy(assembler.nextValue()));
                if (k + 1 < tupleCount) {
                    // the tuple after the target must still assemble correctly
                    Assert.assertArrayEquals(where + " successor", expected.get(k + 1), copy(assembler.nextValue()));
                }
                // the same position reached by reading some tuples and skipping the rest
                reset(pageZero, assembler, providers, streams);
                int read = random.nextInt(k + 1);
                for (int i = 0; i < read; i++) {
                    assembler.nextValue();
                }
                assembler.skip(k - read);
                Assert.assertArrayEquals(where + " after reading " + read, expected.get(k),
                        copy(assembler.nextValue()));
            }
            pageNumber++;
        }
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

    /**
     * Embedding-like records: arrays of 384 doubles where some components print as integers (so the item column is
     * a union of a double column and an integer column), some are null, plus an empty array, a missing field and a
     * null field.
     */
    private static File writeEmbeddingCorpus() throws IOException {
        File dir = new File(OUTPUT_PATH, "skip");
        Files.createDirectories(dir.toPath());
        File file = new File(dir, "embeddings.json");
        Random random = new Random(42);
        try (PrintWriter out = new PrintWriter(file)) {
            for (int r = 0; r < EMBEDDING_RECORDS; r++) {
                StringBuilder sb = new StringBuilder();
                sb.append("{\"id\": ").append(r);
                if (r % 29 == 7) {
                    // field missing entirely
                } else if (r % 29 == 15) {
                    sb.append(", \"emb\": null");
                } else if (r % 29 == 22) {
                    sb.append(", \"emb\": []");
                } else {
                    sb.append(", \"emb\": [");
                    for (int d = 0; d < EMBEDDING_DIMENSIONS; d++) {
                        if (d > 0) {
                            sb.append(", ");
                        }
                        if (r % 5 == 0 && d % 97 == 3) {
                            sb.append(random.nextInt(3));
                        } else if (r % 7 == 0 && d % 131 == 5) {
                            sb.append("null");
                        } else {
                            sb.append(String.format("%.6f", random.nextGaussian()));
                        }
                    }
                    sb.append(']');
                }
                sb.append('}');
                out.println(sb);
            }
        }
        return file;
    }
}
