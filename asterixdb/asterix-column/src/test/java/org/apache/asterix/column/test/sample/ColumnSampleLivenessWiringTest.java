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
package org.apache.asterix.column.test.sample;

import org.apache.asterix.column.operation.lsm.merge.MergeColumnReadMetadata;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTreeOpContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBatchPointSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnSampleLivenessSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Guards both halves of the existence-only liveness optimization — the cheaper cursor and the key-only
 * projection — at their wiring seams.
 * <p>
 * The dangerous failure is not "it stopped working" (the cost tests show that) but "it leaked onto the query
 * path", which a query needs the projected columns and a positioned tuple for and which nothing else here would
 * notice.
 */
public class ColumnSampleLivenessWiringTest {

    /** Small: this test inspects wiring and projection shape, not sampling behaviour. */
    private static final int NUM_KEYS = 20000;
    private static final int SHADOW_PCT = 55;
    private static final int NUM_SHADOW_COMPONENTS = 3;

    private final ColumnSampleBenchHarness harness =
            new ColumnSampleBenchHarness(NUM_KEYS, SHADOW_PCT, NUM_SHADOW_COMPONENTS);

    @Before
    public void setUp() throws Exception {
        harness.setUp();
    }

    @After
    public void tearDown() throws Exception {
        harness.tearDown();
    }

    @Test
    public void sampleLivenessProbeGetsTheExistenceOnlyCursorAndQueriesDoNot() throws Exception {
        LSMColumnBTreeOpContext opCtx = harness.createSearchOpContext();
        LSMBTreeBatchPointSearchCursor livenessCursor = null;
        LSMBTreeBatchPointSearchCursor queryCursor = null;
        try {
            livenessCursor = harness.getIndex().createSampleLivenessSearchCursor(opCtx);
            Assert.assertTrue(
                    "the sample cursor's liveness probe must get the existence-only cursor, got "
                            + livenessCursor.getClass().getName(),
                    livenessCursor instanceof LSMColumnSampleLivenessSearchCursor);

            queryCursor = harness.getIndex().createBatchPointSearchCursor(opCtx);
            // Exact class, not instanceof: the existence-only cursor is a SUBCLASS of this one, so an
            // instanceof check would pass even if the query path had been switched over to it. Query point
            // searches read the projected payload and would silently get nothing.
            Assert.assertEquals("query batch point search must not get the existence-only cursor",
                    LSMColumnBatchPointSearchCursor.class, queryCursor.getClass());
        } finally {
            Throwable failure = CleanupUtils.destroy(null, livenessCursor, queryCursor);
            if (failure != null) {
                throw new AssertionError(failure);
            }
        }
    }

    @Test
    public void existenceOnlyProjectionDropsEveryColumnButKeepsThePrimaryKeys() throws Exception {
        IColumnProjectionInfo full = harness.createSearchOpContext().createProjectionInfo();
        Assert.assertNotNull("expected a disk component to supply the sample projection info", full);
        // The sample collector installs an all-fields projector reporting MODIFY. Asserting MERGE here would
        // pass against a shape production never produces, and keep passing with the reduction fully broken.
        Assert.assertEquals(ColumnProjectorType.MODIFY, full.getProjectorType());
        Assert.assertTrue("phase 2's projection must be a query read projection; got " + full.getClass().getName(),
                full instanceof QueryColumnMetadata);
        Assert.assertTrue("the full sample projection is expected to project every column; got "
                + full.getNumberOfProjectedColumns(), full.getNumberOfProjectedColumns() > 0);

        IColumnProjectionInfo existenceOnly = full.createExistenceOnlyProjectionInfo();
        // The interface default returns `this` -- correct, but not the reduction under test.
        Assert.assertNotSame("expected a reduced view, not the interface's identity fallback", full, existenceOnly);
        Assert.assertEquals("existence-only projection must project no columns", 0,
                existenceOnly.getNumberOfProjectedColumns());
        Assert.assertEquals("existence-only projection must filter no columns", 0,
                existenceOnly.getNumberOfFilteredColumns());
        // The primary keys settle existence, so they must survive untouched.
        Assert.assertEquals(full.getNumberOfPrimaryKeys(), existenceOnly.getNumberOfPrimaryKeys());
        // EXISTENCE, not the parent's MODIFY: CloudColumnReadContext branches on the projector type before it
        // looks at the projected set, so the type is what keeps the reduction real on cloud storage.
        Assert.assertEquals("existence-only projection must report EXISTENCE so cloud reads pin only page zero",
                ColumnProjectorType.EXISTENCE, existenceOnly.getProjectorType());

        // The reduced view is merge-shaped, and a merge projection refuses to be reduced -- nothing should ask
        // it to, so the refusal is what keeps that assumption checked.
        Assert.assertThrows(UnsupportedOperationException.class, existenceOnly::createExistenceOnlyProjectionInfo);
    }

    /**
     * Phase 2 must keep <b>every</b> column: it materializes the sampled tuples into the sample index, so a
     * reduced projection reaching it writes that index with missing column data — silent corruption, the worst
     * outcome available here. Two ways in, both closed:
     * <ol>
     * <li><b>Mutation</b> — a reduction applied in place would reduce the object phase 2 holds, so the full
     * projection is re-inspected <em>after</em> the reduced view is derived.</li>
     * <li><b>Wiring</b> — the reduction belongs only to
     * {@code LSMColumnSampleLivenessSearchCursor#createAccessor}; phase 2's accessor comes from the unreduced
     * {@code createProjectionInfo()}, and this asserts what that path produces.</li>
     * </ol>
     * That phase 2's projection really does still pin the pages it projects is asserted in
     * {@link CloudExistenceProjectionPinningTest}.
     */
    @Test
    public void derivingTheExistenceOnlyViewLeavesPhase2sProjectionIntact() throws Exception {
        LSMColumnBTreeOpContext opCtx = harness.createSearchOpContext();
        // What the sample cursor's own accessor builds its projection from.
        IColumnProjectionInfo phase2Projection = opCtx.createProjectionInfo();
        Assert.assertNotNull("expected a disk component to supply the sample projection info", phase2Projection);
        Assert.assertTrue("phase 2 must be given a query read projection; got " + phase2Projection.getClass().getName(),
                phase2Projection instanceof QueryColumnMetadata);
        QueryColumnMetadata query = (QueryColumnMetadata) phase2Projection;

        int projectedBefore = query.getNumberOfProjectedColumns();
        PrimitiveColumnValuesReader[] keyReadersBefore = query.getPrimaryKeyReaders();
        Assert.assertEquals(ColumnProjectorType.MODIFY, query.getProjectorType());
        Assert.assertTrue("phase 2's projection must project every column; got " + projectedBefore,
                projectedBefore > 0);
        Assert.assertTrue("an all-fields projection must project more than the primary keys",
                projectedBefore > keyReadersBefore.length);

        // Derive the liveness probe's reduced view, then re-check phase 2's projection.
        IColumnProjectionInfo existenceOnly = query.createExistenceOnlyProjectionInfo();
        Assert.assertEquals("existence-only view must be a separate object, leaving phase 2's projection alone",
                ColumnProjectorType.MODIFY, query.getProjectorType());
        Assert.assertEquals("deriving the existence-only view must not reduce phase 2's projected column count",
                projectedBefore, query.getNumberOfProjectedColumns());
        Assert.assertSame("the existence-only view must share, not replace, phase 2's primary-key readers",
                keyReadersBefore, query.getPrimaryKeyReaders());
        IColumnValuesReader[] reducedReaders = ((MergeColumnReadMetadata) existenceOnly).getColumnReaders();
        Assert.assertEquals("the existence-only view must expose the primary keys and nothing else",
                keyReadersBefore.length, reducedReaders.length);
        for (int i = 0; i < keyReadersBefore.length; i++) {
            Assert.assertSame("primary-key reader " + i + " must be shared, not copied", keyReadersBefore[i],
                    reducedReaders[i]);
            Assert.assertEquals("primary key " + i + " must stay bound to column " + i, i,
                    reducedReaders[i].getColumnIndex());
        }
    }
}
