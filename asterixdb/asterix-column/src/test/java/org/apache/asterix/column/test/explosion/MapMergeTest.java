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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.column.common.buffer.DummyBufferCache;
import org.apache.asterix.column.common.buffer.TestWriteMultiPageOp;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MapSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.ColumnTransformer;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.merge.MergeColumnReadMetadata;
import org.apache.asterix.column.operation.lsm.merge.MergeColumnWriteMetadata;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Checks the invariant LSM merge relies on when a node collapses partway through an index's life.
 * <p>
 * Merge never interprets the schema tree: {@link MergeColumnWriteMetadata} deserializes a flat writer list, and
 * {@link MergeColumnReadMetadata} builds one reader per column from the serialized <em>path info</em>. It then copies
 * values by column index. So collapsing is merge-safe exactly when
 * <ol>
 * <li>the newest component's metadata is a superset of every older component's — column indexes are append-only and
 * children discovered before the collapse are never withdrawn, and</li>
 * <li>the collapsed node's key and value columns carry path info describing a repeated group, so merge reads them
 * with the same shape they were written with.</li>
 * </ol>
 * The second point is easy to get wrong: path info snapshots the level and delimiters in force at serialization time,
 * so a key recorded outside the map's bracket would merge as a flat column.
 */
public class MapMergeTest {
    private static final int PAGE_SIZE = 4 * 1024;
    private static final int DOCS_BEFORE = 40;
    private static final int DOCS_AFTER = 60;
    private static final int KEYS_PER_DOCUMENT = 2;

    @Before
    public void lowerThresholds() {
        CollapseSettings.install(CollapseSettings.lowered(8, 1));
    }

    @After
    public void resetThresholds() {
        CollapseSettings.reset();
    }

    @Test
    public void olderComponentIsASubsetOfNewer() throws Exception {
        DummyBufferCache bufferCache = new DummyBufferCache(PAGE_SIZE);
        int fileId = bufferCache.createFile();
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        IColumnValuesWriterFactory writerFactory = new ColumnValuesWriterFactory(multiPageOpRef);
        FlushColumnMetadata columnMetadata = new FlushColumnMetadata(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null,
                Collections.emptyList(), null, writerFactory, multiPageOpRef, CollapseSettings.current());
        columnMetadata.init(new TestWriteMultiPageOp(bufferCache, fileId));
        ColumnTransformer transformer = new ColumnTransformer(columnMetadata, columnMetadata.getRoot(), new BitSet());

        // The index keeps one schema across flushes, so an older component's metadata is simply an earlier snapshot
        // of the same growing structure.
        feed(transformer, generate(0, 4, 0));
        assertTrue("must not have collapsed yet", !columnMetadata.getRoot().isMapified());
        ArrayBackedValueStorage older = copyOf(columnMetadata.serializeColumnsMetadata());
        int olderColumns = columnMetadata.getNumberOfColumns();

        feed(transformer, generate(4, DOCS_BEFORE + DOCS_AFTER, KEYS_PER_DOCUMENT));
        assertTrue("the fixture must collapse; check the lowered thresholds", columnMetadata.getRoot().isMapified());
        ArrayBackedValueStorage newer = copyOf(columnMetadata.serializeColumnsMetadata());
        int newerColumns = columnMetadata.getNumberOfColumns();

        // (1) superset: nothing an older component wrote may disappear from the newest schema
        assertTrue(
                "collapsing must never withdraw a column index already written: " + newerColumns + " < " + olderColumns,
                newerColumns >= olderColumns);

        MergeColumnReadMetadata olderRead = MergeColumnReadMetadata.create(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE,
                null, 0, new ColumnValueReaderFactory(), older);
        MergeColumnReadMetadata newerRead = MergeColumnReadMetadata.create(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE,
                null, 0, new ColumnValueReaderFactory(), newer);
        assertTrue("every column readable in the older component must still be readable in the newer one",
                newerRead.getColumnReaders().length >= olderRead.getColumnReaders().length);
        for (int i = 0; i < olderRead.getColumnReaders().length; i++) {
            assertNotNull("column " + i + " vanished from the newer component's path info",
                    newerRead.getColumnReaders()[i]);
        }

        // the merge writer must be able to write every column the newest schema declares
        MergeColumnWriteMetadata mergeWrite =
                MergeColumnWriteMetadata.create(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null, 0,
                        new MutableObject<>(), newer, new ArrayList<>(), new ArrayList<>());
        assertEquals("merge must allocate one writer per column in the newest schema", newerColumns,
                mergeWrite.getNumberOfColumns());

        // (2) the collapsed node's columns must merge as a repeated group
        MapSchemaNode mapNode = columnMetadata.getRoot().getMapNode();
        IColumnValuesReader keyReader = newerRead.getColumnReaders()[mapNode.getKeyColumnIndex()];
        assertNotNull("the key column is absent from path info", keyReader);
        assertTrue("the key column must merge as a repeated column, not a flat one; path info recorded "
                + keyReader.getNumberOfDelimiters() + " delimiters", keyReader.getNumberOfDelimiters() > 0);

        // The key and the value columns are one repeated group, so merge must see identical repetition for both.
        List<Integer> valueColumns = new ArrayList<>();
        collectColumns(mapNode.getItemNode(), valueColumns);
        assertTrue("the collapsed node must have at least one value column", !valueColumns.isEmpty());
        for (int columnIndex : valueColumns) {
            IColumnValuesReader valueReader = newerRead.getColumnReaders()[columnIndex];
            assertNotNull("value column " + columnIndex + " is absent from path info", valueReader);
            assertEquals("key and value columns of a collapsed node must merge with the same repetition",
                    valueReader.getNumberOfDelimiters(), keyReader.getNumberOfDelimiters());
        }
    }

    private static void collectColumns(AbstractSchemaNode node, List<Integer> out) {
        if (node instanceof PrimitiveSchemaNode) {
            int columnIndex = ((PrimitiveSchemaNode) node).getColumnIndex();
            if (columnIndex >= 0) {
                out.add(columnIndex);
            }
        } else if (node instanceof UnionSchemaNode) {
            for (AbstractSchemaNode child : ((UnionSchemaNode) node).getChildren().values()) {
                collectColumns(child, out);
            }
        } else if (node instanceof ObjectSchemaNode) {
            for (AbstractSchemaNode child : ((ObjectSchemaNode) node).getChildren()) {
                collectColumns(child, out);
            }
        } else if (node instanceof AbstractCollectionSchemaNode) {
            collectColumns(((AbstractCollectionSchemaNode) node).getItemNode(), out);
        }
    }

    private static void feed(ColumnTransformer transformer, List<String> records) throws Exception {
        IStreamDataParser parser = new ADMDataParser(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, true);
        parser.setInputStream(new ByteArrayInputStream(String.join("\n", records).getBytes(StandardCharsets.UTF_8)));
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        RecordLazyVisitablePointable pointable = new RecordLazyVisitablePointable(true);
        storage.reset();
        while (parser.parse(storage.getDataOutput())) {
            pointable.set(storage);
            transformer.transform(pointable);
            storage.reset();
        }
    }

    private static List<String> generate(int from, int to, int keysPerDocument) {
        List<String> lines = new ArrayList<>();
        for (int d = from; d < to; d++) {
            StringBuilder sb = new StringBuilder("{\"id\": ").append(d);
            for (int k = 0; k < keysPerDocument; k++) {
                sb.append(", \"").append(key(d, k)).append("\": {\"a\": ").append(d).append("}");
            }
            lines.add(sb.append("}").toString());
        }
        return lines;
    }

    private static String key(int doc, int index) {
        long h = doc * 0x9E3779B97F4A7C15L + index * 0xC2B2AE3D27D4EB4FL;
        return String.format("k%08x-%04x", (int) (h >>> 32), doc & 0xFFFF);
    }

    private static ArrayBackedValueStorage copyOf(org.apache.hyracks.data.std.api.IValueReference value)
            throws Exception {
        ArrayBackedValueStorage copy = new ArrayBackedValueStorage(value.getLength());
        copy.append(value);
        return copy;
    }
}
