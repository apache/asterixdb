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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read.CloudColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeLeafFrameFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTreeOpContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Evidence that the existence-only projection changes what the <b>cloud</b> read context pins — the one thing
 * reading the code cannot establish.
 * <p>
 * Zero projected columns is enough on premise ({@code DefaultColumnReadContext#prepareColumns} is a no-op), but
 * {@link CloudColumnReadContext} branches on the projector <em>type</em> before it looks at the projected set at
 * all. A key-only view still reporting its parent's type would be silently ignored on cloud storage — the failure
 * this test catches.
 * <p>
 * <b>Runs on premise, still proves a cloud property.</b> Which pages {@code CloudColumnReadContext} asks to pin
 * follows only from its projection info and leaf frame; a cloud IO manager is needed to <em>service</em> those
 * pins, not to decide them. So this builds a real component, positions a real {@link ColumnBTreeReadLeafFrame}
 * on a real mega-leaf, and records what {@code prepareColumns} asks for.
 * <p>
 * The recorder does not forward the read context to
 * {@code IBufferCache#pin(long, IBufferCacheReadContext)}: the cloud context's callbacks cast pages to
 * {@code CloudCachedPage}, which an on-premise cache never produces. It records the page id and pins through the
 * contextless overload, so pages are real and unpinned symmetrically.
 */
public class CloudExistenceProjectionPinningTest {

    private static final Logger LOGGER = LogManager.getLogger();

    /** Small: this test inspects per-leaf pinning decisions, not sampling behaviour. */
    private static final int NUM_KEYS = 20000;
    private static final int SHADOW_PCT = 55;
    private static final int NUM_SHADOW_COMPONENTS = 3;

    /**
     * The most permissive setting, so nothing observed here can be explained away as the drive declining to
     * cache. {@code prepareColumns} never consults it anyway — only page persistence does, which the recorder
     * bypasses.
     */
    private static final IPhysicalDrive UNPRESSURED_DRIVE = new IPhysicalDrive() {
        @Override
        public boolean computeAndCheckIsPressured() {
            return false;
        }

        @Override
        public boolean isUnpressured() {
            return true;
        }

        @Override
        public boolean hasSpace() {
            return true;
        }
    };

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
    public void existenceOnlyProjectionPinsNothingBeyondPageZeroWhereTheFullProjectionPinsItsColumns()
            throws Exception {
        LSMColumnBTreeOpContext opCtx = harness.createSearchOpContext();
        List<ILSMComponent> components = opCtx.getComponentHolder();
        ColumnBTree columnBTree = (ColumnBTree) components.get(components.size() - 1).getIndex();
        IColumnProjectionInfo full = opCtx.createProjectionInfo();
        Assert.assertNotNull("expected a disk component to supply the sample projection info", full);
        // What production's sample collector installs: an all-fields query projector reporting MODIFY.
        Assert.assertEquals(ColumnProjectorType.MODIFY, full.getProjectorType());

        IColumnProjectionInfo existenceOnly = full.createExistenceOnlyProjectionInfo();

        int[] leafPageIds = harness.getSampledComponentLeafPageIds();
        Assert.assertTrue("expected the sampled component to have at least one mega-leaf page", leafPageIds.length > 0);

        // Aggregated over every mega-leaf of the sampled component, not just one, so the numbers reflect what a
        // whole sampling pass over this component would pin.
        int pageZeroOnlyLeaves = 0;
        int actualFullPins = 0;
        int actualExistencePins = 0;
        for (int leafPageId : leafPageIds) {
            PinTrace fullTrace = prepareColumnsOn(columnBTree, full, leafPageId);
            PinTrace existenceTrace = prepareColumnsOn(columnBTree, existenceOnly, leafPageId);
            // The two runs must have seen the same page shape, or the comparison means nothing.
            Assert.assertEquals(fullTrace.megaLeafPages, existenceTrace.megaLeafPages);
            Assert.assertEquals(fullTrace.pageZeroSegments, existenceTrace.pageZeroSegments);
            if (fullTrace.megaLeafPages - fullTrace.pageZeroSegments == 0) {
                pageZeroOnlyLeaves++;
            }
            actualFullPins += fullTrace.pinnedPageIds.size();
            actualExistencePins += existenceTrace.pinnedPageIds.size();
        }

        LOGGER.info(
                "prepareColumns over {} mega-leaf page(s) of the sampled component: the full projection pinned "
                        + "{} page(s), the existence-only projection pinned {}",
                leafPageIds.length, actualFullPins, actualExistencePins);

        // All-single-page mega-leaves would make the two branches indistinguishable and this pass vacuously.
        Assert.assertTrue("this test needs at least one multi-page mega-leaf to distinguish the two branches",
                pageZeroOnlyLeaves < leafPageIds.length);
        // Not an exact count -- MODIFY pins *coalesced* ranges, so the arithmetic follows this component's
        // column layout. That it pins anything at all is what makes the zero below meaningful.
        Assert.assertTrue("the full projection must pin the column pages it projects; pinned " + actualFullPins,
                actualFullPins > 0);
        Assert.assertEquals("the existence-only projection must pin nothing beyond page zero", 0, actualExistencePins);

        // Last, deliberately: the projector type is only the mechanism, so if it is ever replaced the pin
        // counts above are what must keep holding and what should fail first.
        Assert.assertEquals("the existence-only projection is expected to report EXISTENCE",
                ColumnProjectorType.EXISTENCE, existenceOnly.getProjectorType());
    }

    /**
     * Records only what {@code prepareColumns} pins for one projection on one mega-leaf. The page-zero segment
     * pins are recorded and discarded first: both branches pin those, and this is about what lies beyond them.
     */
    private PinTrace prepareColumnsOn(ColumnBTree columnBTree, IColumnProjectionInfo projectionInfo, int leafPageId)
            throws HyracksDataException {
        IBufferCache realBufferCache = harness.getDiskBufferCache();
        RecordingBufferCache recorder = new RecordingBufferCache(realBufferCache);
        IBufferCache recordingBufferCache = recorder.asBufferCache();
        CloudColumnReadContext readContext =
                new CloudColumnReadContext(projectionInfo, UNPRESSURED_DRIVE, new BitSet());
        ColumnBTreeLeafFrameFactory leafFrameFactory = (ColumnBTreeLeafFrameFactory) columnBTree.getLeafFrameFactory();
        ColumnBTreeReadLeafFrame leafFrame = leafFrameFactory.createReadFrame(projectionInfo);
        int fileId = columnBTree.getFileId();
        ICachedPage pageZero = realBufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafPageId));
        try {
            leafFrame.setPage(pageZero);
            readContext.preparePageZeroSegments(leafFrame, recordingBufferCache, fileId);
            int pageZeroSegments = leafFrame.getNumberOfPageZeroSegments();
            int megaLeafPages = leafFrame.getMegaLeafNodeNumberOfPages();
            recorder.clear();
            readContext.prepareColumns(leafFrame, recordingBufferCache, fileId);
            return new PinTrace(megaLeafPages, pageZeroSegments, recorder.pinnedPageIds());
        } finally {
            Throwable failure = null;
            try {
                readContext.close(recordingBufferCache);
            } catch (Throwable t) {
                failure = t;
            }
            try {
                realBufferCache.unpin(pageZero);
            } catch (Throwable t) {
                failure = failure == null ? t : failure;
            }
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
        }
    }

    /** What one {@code prepareColumns} call did, and the page it did it for. */
    private static final class PinTrace {
        private final int megaLeafPages;
        private final int pageZeroSegments;
        private final List<Integer> pinnedPageIds;

        private PinTrace(int megaLeafPages, int pageZeroSegments, List<Integer> pinnedPageIds) {
            this.megaLeafPages = megaLeafPages;
            this.pageZeroSegments = pageZeroSegments;
            this.pinnedPageIds = pinnedPageIds;
        }
    }

    /**
     * An {@link IBufferCache} that records every context-carrying {@code pin} and services it through the
     * contextless overload, forwarding everything else to the real cache. Built as a dynamic proxy so it does not
     * have to restate the ~50 methods of {@link IBufferCache}, only the two whose behaviour matters here.
     */
    private static final class RecordingBufferCache implements InvocationHandler {
        private final IBufferCache delegate;
        private final List<Integer> pinnedPageIds = new ArrayList<>();

        private RecordingBufferCache(IBufferCache delegate) {
            this.delegate = delegate;
        }

        private IBufferCache asBufferCache() {
            return (IBufferCache) Proxy.newProxyInstance(IBufferCache.class.getClassLoader(),
                    new Class<?>[] { IBufferCache.class }, this);
        }

        private void clear() {
            pinnedPageIds.clear();
        }

        private List<Integer> pinnedPageIds() {
            return new ArrayList<>(pinnedPageIds);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (isPinWithContext(method)) {
                long dpid = (Long) args[0];
                pinnedPageIds.add(BufferedFileHandle.getPageId(dpid));
                return delegate.pin(dpid);
            }
            if (isUnpinWithContext(method)) {
                delegate.unpin((ICachedPage) args[0]);
                return null;
            }
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

        private static boolean isPinWithContext(Method method) {
            return "pin".equals(method.getName()) && method.getParameterCount() == 2
                    && method.getParameterTypes()[0] == long.class
                    && method.getParameterTypes()[1] == IBufferCacheReadContext.class;
        }

        private static boolean isUnpinWithContext(Method method) {
            return "unpin".equals(method.getName()) && method.getParameterCount() == 2
                    && method.getParameterTypes()[0] == ICachedPage.class
                    && method.getParameterTypes()[1] == IBufferCacheReadContext.class;
        }
    }
}
