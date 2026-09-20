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
package org.apache.asterix.column.values.reader;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Queue;

import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.common.buffer.NoOpWriteMultiPageOp;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.reader.value.NoOpValueReader;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.junit.Assert;
import org.junit.Test;

/**
 * A NULL or MISSING typed column has level entries but never a value, so its value reader is {@link NoOpValueReader}
 * and a skip over it must ask that reader for zero values and nothing else. Every merge skips every non-key column,
 * and a query skips a superseded record's columns, so this path is hit by ordinary upserts; the assembler-driven skip
 * tests do not reach it because such columns are not assembled.
 */
public class NullMissingColumnSkipTest {
    private static final int MAX_LEVEL = 2;
    private static final int PARENT_LEVEL = MAX_LEVEL - 1;
    private static final int DELIMITER = PARENT_LEVEL - 1;
    private static final int TUPLES = 40;

    @Test
    public void noOpReaderAcceptsAZeroSkipOnly() throws HyracksDataException {
        NoOpValueReader.INSTANCE.skip(0);
        Assert.assertThrows(UnsupportedOperationException.class, () -> NoOpValueReader.INSTANCE.skip(1));
    }

    @Test
    public void skipsMissingColumn() throws HyracksDataException {
        // a field that is missing in some records and null in the others
        byte[] column = write(ATypeTag.MISSING, false, (writer, t) -> {
            if (t % 3 == 0) {
                writer.writeNull(PARENT_LEVEL);
            } else {
                writer.writeLevel(PARENT_LEVEL);
            }
        });
        checkPrimitive(ATypeTag.MISSING, column);
    }

    @Test
    public void skipsNullColumn() throws HyracksDataException {
        byte[] column = write(ATypeTag.NULL, false, (writer, t) -> writer.writeNull(PARENT_LEVEL));
        checkPrimitive(ATypeTag.NULL, column);
    }

    @Test
    public void skipsMissingArrayItemColumn() throws HyracksDataException {
        // the item column every array carries for items of other types: one missing entry per item, then the
        // array's delimiter; some arrays empty, some records without the array at all
        byte[] column = write(ATypeTag.MISSING, true, (writer, t) -> {
            if (t % 7 == 6) {
                writer.writeLevel(DELIMITER);
                return;
            }
            for (int i = 0; i < 1 + t % 5; i++) {
                writer.writeLevel(PARENT_LEVEL);
            }
            writer.writeLevel(DELIMITER);
        });
        int[] delimiters = { DELIMITER };
        for (int k = 0; k < TUPLES; k++) {
            IColumnValuesReader skipped = reader(ATypeTag.MISSING, delimiters, column);
            IColumnValuesReader sequential = reader(ATypeTag.MISSING, delimiters, column);
            skipped.skip(k);
            for (int t = 0; t < k; t++) {
                nextTuple(sequential);
            }
            for (int t = k; t < TUPLES; t++) {
                Assert.assertTrue(skipped.next());
                Assert.assertTrue(sequential.next());
                assertSameEntry("tuple " + t + " after skip(" + k + ")", sequential, skipped);
                // an empty array is a lone delimiter that is not repeated, and ends the tuple by itself
                while (sequential.isRepeatedValue() && !sequential.isLastDelimiter()) {
                    Assert.assertTrue(skipped.next());
                    Assert.assertTrue(sequential.next());
                    assertSameEntry("tuple " + t + " after skip(" + k + ")", sequential, skipped);
                }
            }
            Assert.assertFalse(skipped.next());
        }
    }

    private static void checkPrimitive(ATypeTag typeTag, byte[] column) throws HyracksDataException {
        for (int k = 0; k < TUPLES; k++) {
            IColumnValuesReader skipped = reader(typeTag, null, column);
            IColumnValuesReader sequential = reader(typeTag, null, column);
            skipped.skip(k);
            for (int t = 0; t < k; t++) {
                Assert.assertTrue(sequential.next());
            }
            for (int t = k; t < TUPLES; t++) {
                Assert.assertTrue(skipped.next());
                Assert.assertTrue(sequential.next());
                assertSameEntry("tuple " + t + " after skip(" + k + ")", sequential, skipped);
            }
            Assert.assertFalse(skipped.next());
        }
    }

    private static void nextTuple(IColumnValuesReader reader) throws HyracksDataException {
        Assert.assertTrue(reader.next());
        while (reader.isRepeatedValue() && !reader.isLastDelimiter()) {
            Assert.assertTrue(reader.next());
        }
    }

    private static void assertSameEntry(String where, IColumnValuesReader expected, IColumnValuesReader actual) {
        Assert.assertEquals(where + " level", expected.getLevel(), actual.getLevel());
        Assert.assertEquals(where + " null", expected.isNull(), actual.isNull());
        Assert.assertEquals(where + " missing", expected.isMissing(), actual.isMissing());
        Assert.assertEquals(where + " delimiter", expected.isDelimiter(), actual.isDelimiter());
    }

    private interface TupleWriter {
        void write(IColumnValuesWriter writer, int tuple) throws HyracksDataException;
    }

    private static byte[] write(ATypeTag typeTag, boolean collection, TupleWriter tupleWriter)
            throws HyracksDataException {
        ColumnValuesWriterFactory factory = new ColumnValuesWriterFactory(
                new MutableObject<IColumnWriteMultiPageOp>(NoOpWriteMultiPageOp.INSTANCE));
        IColumnValuesWriter writer = factory.createValueWriter(typeTag, 0, MAX_LEVEL, collection, false);
        for (int t = 0; t < TUPLES; t++) {
            tupleWriter.write(writer, t);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.flush(out);
        return out.toByteArray();
    }

    private static IColumnValuesReader reader(ATypeTag typeTag, int[] delimiters, byte[] column)
            throws HyracksDataException {
        ColumnValueReaderFactory factory = new ColumnValueReaderFactory();
        IColumnValuesReader reader = delimiters == null ? factory.createValueReader(typeTag, 0, MAX_LEVEL, false)
                : factory.createValueReader(typeTag, 0, MAX_LEVEL, delimiters);
        MultiByteBufferInputStream stream = new MultiByteBufferInputStream();
        stream.reset(new BytesProvider(column));
        reader.reset(stream, TUPLES);
        return reader;
    }

    /** Serves a column's bytes in two buffers, so the stream has to cross a boundary. */
    private static final class BytesProvider implements IColumnBufferProvider {
        private final byte[] bytes;

        BytesProvider(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public void reset(ColumnBTreeReadLeafFrame frame) {
            // nothing to do
        }

        @Override
        public void readAll(Queue<ByteBuffer> buffers) {
            int half = bytes.length / 2;
            buffers.add(ByteBuffer.wrap(bytes, 0, half).slice());
            buffers.add(ByteBuffer.wrap(bytes, half, bytes.length - half).slice());
        }

        @Override
        public void releaseAll() {
            // nothing to do
        }

        @Override
        public ByteBuffer getBuffer() {
            return null;
        }

        @Override
        public int getLength() {
            return bytes.length;
        }

        @Override
        public int getColumnIndex() {
            return 0;
        }
    }
}
