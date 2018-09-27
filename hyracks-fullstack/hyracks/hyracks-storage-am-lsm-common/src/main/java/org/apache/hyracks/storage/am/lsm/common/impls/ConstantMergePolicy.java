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

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class ConstantMergePolicy implements ILSMMergePolicy {
    private int numComponents;

    private int[][] binomial;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }
        if (fullMergeIsRequested) {
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleFullMerge();
            return;
        }
        scheduleMerge(index);
    }

    private boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException {
        Optional<Long> latestSeq = ((AbstractLSMIndex) index).getLatestDiskComponentSequence();
        if (!latestSeq.isPresent()) {
            return false;
        }
        // sequence number starts from 0, and thus latestSeq + 1 gives the number of flushes
        int numFlushes = latestSeq.get().intValue() + 1;
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        Collections.reverse(immutableComponents);
        int size = immutableComponents.size();
        int depth = 0;
        while (treeDepth(depth) < numFlushes) {
            depth++;
        }
        int mergedIndex =
                binomialIndex(depth, Math.min(depth, numComponents) - 1, numFlushes - treeDepth(depth - 1) - 1);
        if (mergedIndex == size - 1) {
            return false;
        }
        long mergeSize = 0;
        List<ILSMDiskComponent> mergableComponents = new ArrayList<ILSMDiskComponent>();
        for (int i = mergedIndex; i < immutableComponents.size(); i++) {
            mergeSize = mergeSize + immutableComponents.get(i).getComponentSize();
            mergableComponents.add(immutableComponents.get(i));
        }
        Collections.reverse(mergableComponents);
        ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        accessor.scheduleMerge(mergableComponents);
        return true;
    }

    private int treeDepth(int d) {
        if (d < 0) {
            return 0;
        }
        return treeDepth(d - 1) + binomialChoose(d + Math.min(d, numComponents) - 1, d);
    }

    private int binomialIndex(int d, int h, int t) {
        if (t < 0 || t > binomialChoose(d + h, h)) {
            throw new IllegalStateException("Illegal binomial values");
        }
        if (t == 0) {
            return 0;
        } else if (t < binomialChoose(d + h - 1, h)) {
            return binomialIndex(d - 1, h, t);
        }
        return binomialIndex(d, h - 1, t - binomialChoose(d + h - 1, h)) + 1;
    }

    private int binomialChoose(int n, int k) {
        if (k < 0 || k > n) {
            return 0;
        }
        if (k == 0 || k == n) {
            return 1;
        }
        // For efficiency, binomial is persisted to avoid re-computations for every merge
        if (binomial == null || binomial.length <= n) {
            binomial = new int[n + 1][n + 1];
            for (int r = 0; r <= n; r++) {
                for (int c = 0; c <= r; c++) {
                    if (c == 0 || c == r) {
                        binomial[r][c] = 1;
                    } else {
                        binomial[r][c] = binomial[r - 1][c - 1] + binomial[r - 1][c];
                    }
                }
            }
        }
        return binomial[n][k];
    }

    private boolean areComponentsReadableWritableState(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void configure(Map<String, String> properties) {
        numComponents = Integer.parseInt(properties.get(ConstantMergePolicyFactory.NUM_COMPONENTS));
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        // TODO: for now, we simply block the ingestion when there is an ongoing merge
        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        return isMergeOngoing(immutableComponents);
    }

    private boolean isMergeOngoing(List<ILSMDiskComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }
}
