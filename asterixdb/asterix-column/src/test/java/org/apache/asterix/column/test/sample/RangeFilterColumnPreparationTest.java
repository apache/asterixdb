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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.column.filter.FalseColumnFilterEvaluator;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterEvaluatorFactory;
import org.apache.asterix.column.operation.query.QueryColumnTupleProjector;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read.DefaultColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A mega-leaf rejected by the range filter must not have its columns prepared. On cloud storage
 * {@code IColumnReadContext#prepareColumns} pins (downloads) every projected column of the leaf, so preparing a
 * rejected leaf costs the full column I/O for nothing; the range filter is decided from page zero alone and must
 * gate it.
 * <p>
 * The read context here is the on-premise one wrapped in a counter: which leaves get prepared is decided by the
 * cursor, not by the context, so counting the calls is enough to prove the gate without a cloud IO manager. A
 * dynamic proxy does the counting because {@link DefaultColumnReadContext} is final.
 * <p>
 * Only the range search cursor is exercised; {@code ColumnBtreeSampleCursor} applies the same
 * {@code isColumnReadRequired()} gate before its own {@code prepareColumns} call.
 */
public class RangeFilterColumnPreparationTest {

    private static final int NUM_KEYS = 20000;

    /** A range filter that rejects every mega-leaf, as a time filter beyond every leaf's max would. */
    private static final IColumnRangeFilterEvaluatorFactory REJECT_ALL =
            provider -> FalseColumnFilterEvaluator.INSTANCE;

    private final ColumnSampleBenchHarness harness = new ColumnSampleBenchHarness(NUM_KEYS, 0, 0);

    @Before
    public void setUp() throws Exception {
        harness.setUp();
    }

    @After
    public void tearDown() throws Exception {
        harness.tearDown();
    }

    @Test
    public void columnsArePreparedOnlyForLeavesTheRangeFilterAccepts() throws Exception {
        Scan accepted = scan(NoOpColumnFilterEvaluatorFactory.INSTANCE);
        Scan rejected = scan(REJECT_ALL);

        Assert.assertTrue("expected the component to span several mega-leaves", accepted.preparedLeaves > 1);
        Assert.assertEquals("every key is still yielded when the range filter rejects its leaf", accepted.tuples,
                rejected.tuples);
        Assert.assertEquals("a rejected leaf must not have its columns prepared", 0, rejected.preparedLeaves);
    }

    private Scan scan(IColumnRangeFilterEvaluatorFactory rangeFilter) throws Exception {
        List<ILSMComponent> components = harness.createSearchOpContext().getComponentHolder();
        ILSMComponent component = components.get(components.size() - 1);
        ColumnBTree columnBTree = (ColumnBTree) component.getIndex();
        // The task context is required: without one, QueryColumnMetadata installs no range filter at all
        QueryColumnTupleProjector projector =
                new QueryColumnTupleProjector(harness.getRecordType(), 1, ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE,
                        Collections.emptyMap(), rangeFilter, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                        NoOpWarningCollector.INSTANCE, TestUtils.create(32768), ColumnProjectorType.QUERY);
        IColumnProjectionInfo projectionInfo =
                projector.createProjectionInfo(ColumnUtil.getColumnMetadataCopy(component.getMetadata()));
        Scan result = new Scan();
        IColumnReadContext countingContext = countPrepareColumns(result);
        BTreeAccessor accessor =
                columnBTree.createAccessor(NoOpIndexAccessParameters.INSTANCE, 0, projectionInfo, countingContext);
        ITreeIndexCursor cursor = accessor.createSearchCursor(false);
        try {
            accessor.search(cursor, new RangePredicate(null, null, true, true, null, null));
            while (cursor.hasNext()) {
                cursor.next();
                result.tuples++;
            }
        } finally {
            cursor.close();
            cursor.destroy();
        }
        return result;
    }

    private static IColumnReadContext countPrepareColumns(Scan scan) {
        IColumnReadContext delegate = DefaultColumnReadContext.INSTANCE;
        return (IColumnReadContext) Proxy.newProxyInstance(IColumnReadContext.class.getClassLoader(),
                new Class<?>[] { IColumnReadContext.class }, (proxy, method, args) -> {
                    if ("prepareColumns".equals(method.getName())) {
                        scan.preparedLeaves++;
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static final class Scan {
        private int preparedLeaves;
        private int tuples;
    }
}
