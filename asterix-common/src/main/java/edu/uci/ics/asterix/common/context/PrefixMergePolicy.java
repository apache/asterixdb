/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.common.context;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractDiskLSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;

public class PrefixMergePolicy implements ILSMMergePolicy {

    private final long maxMergableComponentSize;
    private final int maxTolernaceComponentCount;
    private final IAsterixAppRuntimeContext ctx;

    public PrefixMergePolicy(long maxMergableComponentSize, int maxTolernaceComponentCount,
            IAsterixAppRuntimeContext ctx) {
        this.maxMergableComponentSize = maxMergableComponentSize;
        this.maxTolernaceComponentCount = maxTolernaceComponentCount;
        this.ctx = ctx;
    }

    @Override
    public void diskComponentAdded(final ILSMIndex index) throws HyracksDataException, IndexException {
        if (!ctx.isShuttingdown()) {
            // 1.  Look at the candidate components for merging in oldest-first order.  If one exists, identify the prefix of the sequence of
            // all such components for which the sum of their sizes exceeds MaxMrgCompSz.  Schedule a merge of those components into a new component.
            // 2.  If a merge from 1 doesn't happen, see if the set of candidate components for merging exceeds MaxTolCompCnt.  If so, schedule
            // a merge all of the current candidates into a new single component.
            List<ILSMComponent> immutableComponents = new ArrayList<ILSMComponent>(index.getImmutableComponents());
            long totalSize = 0;
            int startIndex = -1;
            for (int i = 0; i < immutableComponents.size(); i++) {
                ILSMComponent c = immutableComponents.get(i);
                long componentSize = ((AbstractDiskLSMComponent) c).getComponentSize();
                if (componentSize > maxMergableComponentSize) {
                    startIndex = i;
                    continue;
                }
                totalSize += componentSize;
                boolean isLastComponent = i + 1 == immutableComponents.size() ? true : false;
                if (totalSize > maxMergableComponentSize
                        || (isLastComponent && i - startIndex >= maxTolernaceComponentCount)) {
                    List<ILSMComponent> mergableCopments = new ArrayList<ILSMComponent>();
                    for (int j = startIndex + 1; j <= i; j++) {
                        mergableCopments.add(immutableComponents.get(j));
                    }
                    ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(
                            NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                    accessor.scheduleMerge(NoOpIOOperationCallback.INSTANCE, mergableCopments);
                    break;
                }
            }
        }
    }
}
