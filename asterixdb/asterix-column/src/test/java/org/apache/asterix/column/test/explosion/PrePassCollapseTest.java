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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.common.row.DummyLSMBTreeTupleReference;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleWriter;
import org.apache.asterix.column.operation.lsm.flush.NoWriteColumnTransformer;
import org.apache.asterix.column.operation.lsm.flush.NoWriteFlushColumnMetadata;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.write.DefaultColumnWriteContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A flush sizes each page from a pre-pass over its records. A name a collapsed node routes to its map takes the map's
 * columns, not one of its own: counted as a column, every routed name would size pages as if the node had never
 * collapsed.
 */
public class PrePassCollapseTest {

    @Before
    public void configure() {
        CollapseSettings.install(CollapseSettings.lowered(16, 1));
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    @Test
    public void aNameRoutedToTheMapNeedsNoColumnInTheFlushPrePass() throws Exception {
        FlushColumnMetadata md = WideValueCollapseTest.schemaOf(keyedRecords(0, 200));
        assertTrue("the fixture must collapse the root", md.getRoot().isCollapsed());
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        NoWriteFlushColumnMetadata prePass = NoWriteFlushColumnMetadata.createMutableMetadata(
                DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null, 0, false,
                new ColumnValuesWriterFactory(multiPageOpRef), multiPageOpRef, md.serializeColumnsMetadata());
        NoWriteColumnTransformer transformer = new NoWriteColumnTransformer(prePass, prePass.getRoot(), null);
        int columns = prePass.getNumberOfColumns();
        RecordLazyVisitablePointable pointable = new RecordLazyVisitablePointable(true);
        for (IValueReference record : ColumnFlushHarness.parse(keyedRecords(200, 100))) {
            pointable.set(record);
            transformer.transform(pointable);
        }
        assertEquals("a hundred new names go to the map", columns, prePass.getNumberOfColumns());
    }

    /**
     * The flush's pre-pass works on a copy of the schema taken as the flush starts. A node that collapses during the
     * flush routes its new names to its map from then on; a copy that cannot follow would keep minting a column per
     * name, and size pages and reserve buffers for columns the flush never writes. So it is copied again.
     */
    @Test
    public void thePrePassFollowsACollapseDuringTheFlush() throws Exception {
        ColumnFlushHarness harness = new ColumnFlushHarness();
        FlushColumnMetadata md = harness.freshMetadata(harness.createFile());
        FlushColumnTupleWriter writer = new FlushColumnTupleWriter(md, ColumnFlushHarness.PAGE_SIZE, 1000, 0.15,
                512 * 1024, DefaultColumnWriteContext.INSTANCE);
        DummyLSMBTreeTupleReference tuple = new DummyLSMBTreeTupleReference();
        try {
            for (IValueReference record : ColumnFlushHarness.parse(keyedRecords(0, 300))) {
                tuple.set(record);
                writer.updateColumnMetadataForCurrentTuple(tuple);
                writer.writeTuple(tuple);
            }
            assertTrue("the fixture must collapse the root during the flush", md.getRoot().isCollapsed());
            assertEquals("the pre-pass must count the columns the flush writes",
                    writer.getAbsoluteNumberOfColumns(false), writer.getAbsoluteNumberOfColumns(true));
        } finally {
            writer.close();
        }
    }

    /** Records keyed anew by each: record d holds {@code k<d>}. */
    private static List<String> keyedRecords(int from, int count) {
        List<String> lines = new ArrayList<>();
        for (int d = from; d < from + count; d++) {
            lines.add("{\"id\": " + d + ", \"k" + d + "\": " + d + "}");
        }
        return lines;
    }
}
