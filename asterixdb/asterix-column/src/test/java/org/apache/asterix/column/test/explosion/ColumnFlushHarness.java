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
package org.apache.asterix.column.test.explosion;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.common.buffer.DummyBufferCache;
import org.apache.asterix.column.common.buffer.DummyPage;
import org.apache.asterix.column.common.buffer.TestWriteMultiPageOp;
import org.apache.asterix.column.common.row.DummyLSMBTreeTupleReference;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.metadata.schema.CollapseDetectorSettings;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleWriter;
import org.apache.asterix.column.operation.query.ColumnAssembler;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.test.bytes.components.TestColumnBufferProvider;
import org.apache.asterix.column.util.SchemaJSONBuilderVisitor;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.formats.nontagged.CleanJSONPrinterFactoryProvider;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.NoOpWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.write.DefaultColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Writes columnar components through the flush path and reads them back, so a test can follow a schema across flushes
 * -- through its serialized metadata, as the next flush loads it -- without standing up an LSM index.
 */
public class ColumnFlushHarness {
    public static final int PAGE_SIZE = 4 * 1024;
    private static final int MAX_TUPLES = 1000;
    private static final double TOLERANCE = 0.15;
    private static final int MAX_LEAF_NODE_SIZE = 512 * 1024;
    private static final int TUPLE_COUNT_OFFSET = AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET;

    /** A type that declares its primary key, for components written as a keyed dataset writes them. */
    public static final ARecordType KEYED_TYPE =
            new ARecordType("TestType", new String[] { "id" }, new IAType[] { BuiltinType.AINT64 }, true);
    private static final List<List<String>> PRIMARY_KEYS = Collections.singletonList(Collections.singletonList("id"));
    private static final List<Integer> KEY_SOURCE = Collections.singletonList(0);

    private final DummyBufferCache bufferCache;

    public ColumnFlushHarness() {
        this(PAGE_SIZE);
    }

    /**
     * A harness whose buffer cache has pages of {@code pageSize}, for a test that writes pages itself and only reads
     * them back through this harness. {@link #write} keeps sizing pages by {@link #PAGE_SIZE}.
     */
    public ColumnFlushHarness(int pageSize) {
        bufferCache = new DummyBufferCache(pageSize);
    }

    /** One written component: its pages and the schema that was serialized into it. */
    public static final class Component {
        final int fileId;
        final List<DummyPage> pageZeros;
        final ArrayBackedValueStorage serializedMetadata;
        final int columnCount;

        Component(int fileId, List<DummyPage> pageZeros, ArrayBackedValueStorage serializedMetadata, int columnCount) {
            this.fileId = fileId;
            this.pageZeros = pageZeros;
            this.serializedMetadata = serializedMetadata;
            this.columnCount = columnCount;
        }

        /** The schema this component was written with, as the next flush loads it. */
        public org.apache.hyracks.data.std.api.IValueReference serializedMetadata() {
            return serializedMetadata;
        }

        public int getColumnCount() {
            return columnCount;
        }
    }

    /** Writes ADM lines into a component of their own, with a schema of its own. */
    public Component writeComponent(List<String> records) throws Exception {
        int fileId = bufferCache.createFile();
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        IColumnValuesWriterFactory writerFactory = new ColumnValuesWriterFactory(multiPageOpRef);
        FlushColumnMetadata metadata = new FlushColumnMetadata(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null,
                Collections.emptyList(), null, writerFactory, multiPageOpRef, CollapseSettings.current());
        metadata.init(new TestWriteMultiPageOp(bufferCache, fileId));
        FlushColumnTupleWriter writer = new FlushColumnTupleWriter(metadata, PAGE_SIZE, MAX_TUPLES, TOLERANCE,
                MAX_LEAF_NODE_SIZE, DefaultColumnWriteContext.INSTANCE);
        try {
            List<DummyPage> pages = write(fileId, writer, parse(records));
            ArrayBackedValueStorage serialized = copyOf(metadata.serializeColumnsMetadata());
            return new Component(fileId, pages, serialized, metadata.getNumberOfColumns());
        } finally {
            writer.close();
        }
    }

    /** Reads a component back as whole records, through {@link QueryColumnMetadata} over ALL_FIELDS. */
    public List<IValueReference> assembleRecords(Component component) throws Exception {
        return assemble(component, DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, 0);
    }

    private List<IValueReference> assemble(Component component, ARecordType datasetType, int numberOfPrimaryKeys)
            throws Exception {
        QueryColumnMetadata readMetadata = QueryColumnMetadata.create(datasetType, numberOfPrimaryKeys,
                component.serializedMetadata, new ColumnValueReaderFactory(), ValueGetterFactory.INSTANCE,
                ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE, Collections.emptyMap(),
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpWarningCollector.INSTANCE, null, ColumnProjectorType.QUERY);
        ColumnAssembler assembler = readMetadata.getAssembler();
        int columns = readMetadata.getNumberOfColumns();

        TestColumnBufferProvider[] providers = new TestColumnBufferProvider[columns];
        MultiByteBufferInputStream[] streams = new MultiByteBufferInputStream[columns];
        for (int i = 0; i < columns; i++) {
            providers[i] = new TestColumnBufferProvider(component.fileId, assembler.getColumnIndex(i), bufferCache);
            streams[i] = new MultiByteBufferInputStream();
        }

        List<IValueReference> out = new ArrayList<>();
        for (DummyPage pageZero : component.pageZeros) {
            for (int i = 0; i < columns; i++) {
                providers[i].reset(pageZero);
                streams[i].reset(providers[i]);
            }
            assembler.reset(pageZero.getBuffer().getInt(TUPLE_COUNT_OFFSET));
            for (int i = 0; i < columns; i++) {
                assembler.resetColumn(streams[i], i);
            }
            while (assembler.hasNext()) {
                out.add(copyOf(assembler.nextValue()));
            }
        }
        return out;
    }

    public DummyBufferCache getBufferCache() {
        return bufferCache;
    }

    public int createFile() {
        return bufferCache.createFile();
    }

    /** A fresh, empty schema, as the first flush of a new index starts from. */
    public FlushColumnMetadata freshMetadata(int fileId) throws HyracksDataException {
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        IColumnValuesWriterFactory writerFactory = new ColumnValuesWriterFactory(multiPageOpRef);
        FlushColumnMetadata metadata = new FlushColumnMetadata(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null,
                Collections.emptyList(), null, writerFactory, multiPageOpRef, CollapseSettings.current());
        metadata.init(new TestWriteMultiPageOp(bufferCache, fileId));
        return metadata;
    }

    /**
     * Carries an existing schema forward into the next flush, which is what a real index does: every flush
     * deserializes the index-level schema, adds to it, and writes it back. {@link #freshMetadata} starts from nothing
     * instead.
     * <p>
     * Deserializing also rebuilds the schema nodes, so each flush's nodes read the collapse-detector settings as they
     * stand at that moment -- which is the only way a test can have the detector off for one component and on for the
     * next while keeping one append-only column numbering across both.
     */
    public FlushColumnMetadata continueMetadata(int fileId, IValueReference serializedSchema)
            throws HyracksDataException {
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        FlushColumnMetadata metadata = FlushColumnMetadata.create(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null, 0,
                null, new ColumnValuesWriterFactory(multiPageOpRef), multiPageOpRef, serializedSchema,
                CollapseSettings.current());
        metadata.init(new TestWriteMultiPageOp(bufferCache, fileId));
        return metadata;
    }

    /** A flush that writes through a schema handed to it, rather than one built from scratch. */
    public Component writeComponentWith(FlushColumnMetadata metadata, int fileId, List<String> records)
            throws Exception {
        FlushColumnTupleWriter writer = new FlushColumnTupleWriter(metadata, PAGE_SIZE, MAX_TUPLES, TOLERANCE,
                MAX_LEAF_NODE_SIZE, DefaultColumnWriteContext.INSTANCE);
        try {
            List<IValueReference> parsed =
                    metadata.getNumberOfPrimaryKeys() > 0 ? parse(records, KEYED_TYPE) : parse(records);
            List<DummyPage> pages = write(fileId, writer, parsed);
            return new Component(fileId, pages, copyOf(metadata.serializeColumnsMetadata()),
                    metadata.getNumberOfColumns());
        } finally {
            writer.close();
        }
    }

    /**
     * Writes the next component through {@code metadata} itself, as an index does between activations: it keeps one
     * flush metadata, so its schema nodes carry their state from one flush into the next.
     */
    public Component writeNextComponent(FlushColumnMetadata metadata, List<String> records) throws Exception {
        int fileId = createFile();
        metadata.init(new TestWriteMultiPageOp(bufferCache, fileId));
        return writeComponentWith(metadata, fileId, records);
    }

    /** {@link #freshMetadata} with a declared primary key. */
    public FlushColumnMetadata freshKeyedMetadata(int fileId) throws HyracksDataException {
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        FlushColumnMetadata metadata = new FlushColumnMetadata(KEYED_TYPE, null, PRIMARY_KEYS, KEY_SOURCE,
                new ColumnValuesWriterFactory(multiPageOpRef), multiPageOpRef, CollapseSettings.current());
        metadata.init(new TestWriteMultiPageOp(bufferCache, fileId));
        return metadata;
    }

    /** {@link #continueMetadata} for a keyed schema. */
    public FlushColumnMetadata continueKeyedMetadata(int fileId, IValueReference serializedSchema)
            throws HyracksDataException {
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        FlushColumnMetadata metadata = FlushColumnMetadata.create(KEYED_TYPE, null, PRIMARY_KEYS.size(), KEY_SOURCE,
                new ColumnValuesWriterFactory(multiPageOpRef), multiPageOpRef, serializedSchema,
                CollapseSettings.current());
        metadata.init(new TestWriteMultiPageOp(bufferCache, fileId));
        return metadata;
    }

    public List<DummyPage> write(int fileId, AbstractColumnTupleWriter writer, List<IValueReference> records)
            throws IOException {
        DummyLSMBTreeTupleReference tuple = new DummyLSMBTreeTupleReference();
        List<DummyPage> pageZeros = new ArrayList<>();
        ByteBuffer pageZero = allocate(pageZeros, fileId);
        int tupleCount = 0;
        for (IValueReference record : records) {
            tuple.set(record);
            if (isFull(writer, tupleCount, tuple)) {
                writeFullPage(pageZero, writer, tupleCount);
                pageZero = allocate(pageZeros, fileId);
                tupleCount = 0;
            }
            writer.writeTuple(tuple);
            tupleCount++;
        }
        if (tupleCount > 0) {
            writeFullPage(pageZero, writer, tupleCount);
        }
        return pageZeros;
    }

    public static List<IValueReference> parse(List<String> records) throws Exception {
        return parse(records, DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
    }

    /**
     * Records must be parsed against the type they will be written under. Parsed as fully open but written under
     * {@link #KEYED_TYPE}, the declared "id" has no closed slot in the physical record and the transformer reads the
     * primary key from whatever lies at that offset.
     */
    public static List<IValueReference> parse(List<String> records, ARecordType type) throws Exception {
        IStreamDataParser parser = new ADMDataParser(type, true);
        parser.setInputStream(new ByteArrayInputStream(String.join("\n", records).getBytes(StandardCharsets.UTF_8)));
        List<IValueReference> out = new ArrayList<>();
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        storage.reset();
        while (parser.parse(storage.getDataOutput())) {
            out.add(copyOf(storage));
            storage.reset();
        }
        return out;
    }

    private ByteBuffer allocate(List<DummyPage> pageZeros, int fileId) {
        DummyPage page = bufferCache.allocate(fileId);
        pageZeros.add(page);
        return page.getBuffer();
    }

    private boolean isFull(AbstractColumnTupleWriter writer, int tupleCount, ITupleReference tuple) {
        if (tupleCount == 0) {
            return false;
        } else if (tupleCount >= writer.getMaxNumberOfTuples()) {
            return true;
        }
        int requiredFreeSpace = AbstractColumnBTreeLeafFrame.HEADER_SIZE;
        // DefaultColumnPageZeroWriter is used below, so buffer capacity does not enter the calculation.
        requiredFreeSpace += writer.getPageZeroWriterOccupiedSpace(100, Integer.MAX_VALUE, true,
                IColumnPageZeroWriter.ColumnPageZeroWriterType.DEFAULT);
        requiredFreeSpace += writer.getPrimaryKeysEstimatedSize();
        requiredFreeSpace += writer.bytesRequired(tuple);
        return PAGE_SIZE <= requiredFreeSpace;
    }

    private void writeFullPage(ByteBuffer pageZero, AbstractColumnTupleWriter writer, int tupleCount)
            throws HyracksDataException {
        pageZero.clear();
        DefaultColumnPageZeroWriter pageZeroWriter = new DefaultColumnPageZeroWriter();
        pageZeroWriter.flush(pageZero, tupleCount, writer);
    }

    /**
     * {@link #render} decodes the record bytes as Latin-1, which is only good for substring checks;
     * parsing values back needs the real printer.
     */
    static String json(IValueReference record) throws HyracksDataException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(bytes, false, StandardCharsets.UTF_8)) {
            CleanJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(null).createPrinter(null)
                    .print(record.getByteArray(), record.getStartOffset(), record.getLength(), out);
        }
        return bytes.toString(StandardCharsets.UTF_8);
    }

    /** Every test document {@code d} carries {@code "k<d>": d}; a misread column breaks that pairing. */
    static void assertKeysIntact(List<IValueReference> records) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        for (IValueReference record : records) {
            JsonNode node = mapper.readTree(json(record));
            int id = node.get("id").asInt();
            assertEquals("k" + id + " of document " + id, id, node.path("k" + id).asInt(-1));
        }
    }

    /** Column index -> "path:type", from the same JSON the schema dumps use. */
    static Map<Integer, String> columnsByIndex(FlushColumnMetadata md) throws Exception {
        String json = new SchemaJSONBuilderVisitor(md.getFieldNamesDictionary()).build(md.getRoot());
        Map<Integer, String> out = new TreeMap<>();
        walk(new ObjectMapper().readTree(json), "$", out);
        return out;
    }

    private static void walk(JsonNode node, String path, Map<Integer, String> out) {
        if (node.isObject()) {
            if (node.has("index") && node.get("index").isInt()) {
                out.put(node.get("index").asInt(), path + ":" + node.path("type").asText());
            }
            node.fields().forEachRemaining(e -> walk(e.getValue(), path + "/" + e.getKey(), out));
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                walk(node.get(i), path + "[" + i + "]", out);
            }
        }
    }

    /**
     * The record's raw bytes as a string, for asserting that field names survived a rebuild. Field names are stored
     * as UTF-8 inside the record, so a substring check is enough to tell whether the nesting is still there -- and it
     * avoids standing up a printer for a shape assertion.
     */
    static String render(IValueReference record) {
        return new String(record.getByteArray(), record.getStartOffset(), record.getLength(),
                java.nio.charset.StandardCharsets.ISO_8859_1);
    }

    static ArrayBackedValueStorage copyOf(IValueReference value) throws HyracksDataException {
        ArrayBackedValueStorage copy = new ArrayBackedValueStorage(value.getLength());
        copy.append(value);
        return copy;
    }

    /**
     * Runs {@code body} with the collapse detector's thresholds lowered, so small fixtures collapse: windows of eight
     * documents, four new names making a window fast.
     */
    static void withDetector(int minChildren, int growingWindows, ThrowingRunnable body) throws Exception {
        CollapseDetectorSettings previous = CollapseSettings.current();
        CollapseSettings.install(CollapseSettings.lowered(minChildren, growingWindows));
        try {
            body.run();
        } finally {
            CollapseSettings.install(previous);
        }
    }

    interface ThrowingRunnable {
        void run() throws Exception;
    }
}
