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

import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.HEADER_SIZE;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NUMBER_OF_COLUMNS_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NUMBER_OF_COLUMN_PAGES;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.common.buffer.DummyBufferCache;
import org.apache.asterix.column.common.buffer.DummyPage;
import org.apache.asterix.column.common.buffer.TestWriteMultiPageOp;
import org.apache.asterix.column.common.row.DummyLSMBTreeTupleReference;
import org.apache.asterix.column.common.row.NoOpRowTupleWriter;
import org.apache.asterix.column.common.test.TestBase;
import org.apache.asterix.column.common.test.TestCase;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleWriter;
import org.apache.asterix.column.operation.query.ColumnAssembler;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.test.bytes.components.TestColumnBufferProvider;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.printer.json.clean.APrintVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractBytesTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger();
    /* ***************************************
     * Writer configuration
     * ***************************************
     */
    /**
     * Default is 4KB pages
     */
    public final static int PAGE_SIZE = 4 * 1024;
    /**
     * Empty space tolerance
     */
    public static final float TOLERANCE = 0.15f;
    /**
     * Cap the maximum number of tuples stored per AMAX page
     */
    public static final int MAX_NUMBER_OF_TUPLES = 100;
    /**
     * Max size of the mega leaf node
     */
    public static final int MAX_LEAF_NODE_SIZE = StorageUtil.getIntSizeInBytes(512, StorageUtil.StorageUnit.KILOBYTE);

    /* ***************************************
     * Test static instances
     * ***************************************
     */
    /**
     * NoOp row tuple writer
     */
    public static final ITreeIndexTupleWriter ROW_TUPLE_WRITER = new NoOpRowTupleWriter();

    /* ***************************************
     * Test member fields
     * ***************************************
     */
    protected final DummyBufferCache dummyBufferCache;
    private final ARecordVisitablePointable recordPointable;
    private final APrintVisitor printVisitor;

    protected AbstractBytesTest(TestCase testCase) throws HyracksDataException {
        super(testCase);
        dummyBufferCache = new DummyBufferCache(PAGE_SIZE);
        recordPointable = new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        printVisitor = new APrintVisitor();
    }

    protected int createFile() {
        return dummyBufferCache.createFile();
    }

    protected FlushColumnMetadata prepareNewFile(int fileId) throws HyracksDataException {
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        IColumnValuesWriterFactory writerFactory = new ColumnValuesWriterFactory(multiPageOpRef);
        FlushColumnMetadata columnMetadata = new FlushColumnMetadata(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null,
                Collections.emptyList(), null, writerFactory, multiPageOpRef);
        IColumnWriteMultiPageOp multiPageOp = new TestWriteMultiPageOp(dummyBufferCache, fileId);
        columnMetadata.init(multiPageOp);
        return columnMetadata;
    }

    protected void clear() {
        dummyBufferCache.clear();
    }

    protected List<IValueReference> getParsedRecords() throws IOException {
        List<IValueReference> records = new ArrayList<>();
        prepareParser(testCase.getTestFile());

        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        while (parser.parse(storage.getDataOutput())) {
            records.add(storage);
            storage = new ArrayBackedValueStorage();
        }
        return records;
    }

    /* *************************************************
     * Write
     * *************************************************
     */
    protected List<DummyPage> transform(int fileId, FlushColumnMetadata columnMetadata, List<IValueReference> records,
            int numberOfTuplesToWrite) throws IOException {
        IColumnWriteMultiPageOp multiPageOp = columnMetadata.getMultiPageOpRef().getValue();
        FlushColumnTupleWriter writer = new FlushColumnTupleWriter(columnMetadata, PAGE_SIZE, MAX_NUMBER_OF_TUPLES,
                TOLERANCE, MAX_LEAF_NODE_SIZE);

        try {
            return writeTuples(fileId, writer, records, numberOfTuplesToWrite, multiPageOp);
        } finally {
            writer.close();
        }
    }

    private List<DummyPage> writeTuples(int fileId, AbstractColumnTupleWriter writer, List<IValueReference> records,
            int numberOfTuplesToWrite, IColumnWriteMultiPageOp multiPageOp) throws IOException {

        DummyLSMBTreeTupleReference tuple = new DummyLSMBTreeTupleReference();
        List<DummyPage> pageZeroList = new ArrayList<>();
        ByteBuffer pageZero = allocate(pageZeroList, fileId);
        int tupleCount = 0;
        for (int i = 0; i < numberOfTuplesToWrite; i++) {
            tuple.set(records.get(i % records.size()));
            if (isFull(writer, tupleCount, tuple)) {
                writeFullPage(pageZero, writer, tupleCount, multiPageOp);
                pageZero = allocate(pageZeroList, fileId);
                tupleCount = 0;
            }
            writer.writeTuple(tuple);
            tupleCount++;
        }

        //Flush remaining tuples
        if (tupleCount > 0) {
            writeFullPage(pageZero, writer, tupleCount, multiPageOp);
        }
        return pageZeroList;
    }

    protected void writeFullPage(ByteBuffer pageZero, AbstractColumnTupleWriter writer, int tupleCount,
            IColumnWriteMultiPageOp multiPageOp) throws HyracksDataException {
        pageZero.clear();
        //Reserve the header space
        pageZero.position(HEADER_SIZE);
        writer.flush(pageZero);
        //Write page header
        int numberOfColumn = writer.getNumberOfColumns();
        int numberOfColumnsPages = multiPageOp.getNumberOfPersistentBuffers() - 1;
        pageZero.putInt(TUPLE_COUNT_OFFSET, tupleCount);
        pageZero.putInt(NUMBER_OF_COLUMNS_OFFSET, numberOfColumn);
        pageZero.putInt(NUMBER_OF_COLUMN_PAGES, numberOfColumnsPages);
    }

    protected boolean isFull(AbstractColumnTupleWriter columnWriter, int tupleCount, ITupleReference tuple) {
        if (tupleCount == 0) {
            return false;
        } else if (tupleCount >= columnWriter.getMaxNumberOfTuples()) {
            //We reached the maximum number of tuples
            return true;
        }
        //Reserved for the number of pages
        int requiredFreeSpace = HEADER_SIZE;
        //Columns' Offsets
        requiredFreeSpace += columnWriter.getColumnOffsetsSize();
        //Occupied space from previous writes
        requiredFreeSpace += columnWriter.getOccupiedSpace();
        //New tuple required space
        requiredFreeSpace += columnWriter.bytesRequired(tuple);
        return PAGE_SIZE <= requiredFreeSpace;
    }

    protected ByteBuffer allocate(List<DummyPage> pageZeroList, int fileId) {
        DummyPage page = dummyBufferCache.allocate(fileId);
        pageZeroList.add(page);
        return page.getBuffer();
    }

    /* *************************************************
     * Read
     * *************************************************
     */

    protected void writeResult(int fileId, QueryColumnMetadata readMetadata, List<DummyPage> pageZeroList)
            throws IOException {
        int numberOfColumns = readMetadata.getNumberOfColumns();
        ColumnAssembler assembler = readMetadata.getAssembler();
        TestColumnBufferProvider[] providers = createBufferProviders(fileId, numberOfColumns, assembler);
        MultiByteBufferInputStream[] streams = createInputStreams(numberOfColumns);
        writeResult(pageZeroList, assembler, providers, streams);
    }

    private int prepareRead(DummyPage pageZero, TestColumnBufferProvider[] providers,
            MultiByteBufferInputStream[] streams) throws HyracksDataException {
        for (int i = 0; i < providers.length; i++) {
            TestColumnBufferProvider provider = providers[i];
            MultiByteBufferInputStream stream = streams[i];
            provider.reset(pageZero);
            stream.reset(provider);
        }
        return pageZero.getBuffer().getInt(TUPLE_COUNT_OFFSET);
    }

    private TestColumnBufferProvider[] createBufferProviders(int fileId, int size, ColumnAssembler assembler) {
        TestColumnBufferProvider[] providers = new TestColumnBufferProvider[size];
        for (int i = 0; i < size; i++) {
            int columnIndex = assembler.getColumnIndex(i);
            providers[i] = new TestColumnBufferProvider(fileId, columnIndex, dummyBufferCache);
        }
        return providers;
    }

    private MultiByteBufferInputStream[] createInputStreams(int size) {
        MultiByteBufferInputStream[] streams = new MultiByteBufferInputStream[size];
        for (int i = 0; i < size; i++) {
            streams[i] = new MultiByteBufferInputStream();
        }
        return streams;
    }

    private void writeResult(List<DummyPage> pageZeroList, ColumnAssembler assembler,
            TestColumnBufferProvider[] providers, MultiByteBufferInputStream[] streams)
            throws FileNotFoundException, HyracksDataException {
        File resultFile = testCase.getOutputFile();

        try (PrintStream ps = new PrintStream(new FileOutputStream(resultFile))) {
            int pageNumber = 0;
            for (DummyPage pageZero : pageZeroList) {
                LOGGER.info("READ PageZero {}", pageNumber++);
                assembler.reset(prepareRead(pageZero, providers, streams));
                for (int i = 0; i < streams.length; i++) {
                    assembler.resetColumn(streams[i], i);
                }
                writeForPageZero(ps, assembler);
            }
        }
    }

    private void writeForPageZero(PrintStream ps, ColumnAssembler assembler) throws HyracksDataException {
        Pair<PrintStream, ATypeTag> pair = new Pair<>(ps, ATypeTag.OBJECT);
        while (assembler.hasNext()) {
            IValueReference record = assembler.nextValue();
            recordPointable.set(record);
            recordPointable.accept(printVisitor, pair);
            ps.println();
        }
    }

}
