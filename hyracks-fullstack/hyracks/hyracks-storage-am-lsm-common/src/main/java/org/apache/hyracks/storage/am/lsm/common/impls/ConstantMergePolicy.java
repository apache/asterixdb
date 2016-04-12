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

import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class ConstantMergePolicy implements ILSMMergePolicy {
    private int numComponents;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested)
            throws HyracksDataException, IndexException {
        List<ILSMComponent> immutableComponents = index.getImmutableComponents();

        if (!areComponentsMergable(immutableComponents)) {
            return;
        }

        if (fullMergeIsRequested) {
            ILSMIndexAccessor accessor = index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleFullMerge(index.getIOOperationCallback());
        } else if (immutableComponents.size() >= numComponents) {
            ILSMIndexAccessor accessor = index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleMerge(index.getIOOperationCallback(), immutableComponents);
        }
    }

    @Override
    public void configure(Map<String, String> properties) {
        numComponents = Integer.parseInt(properties.get("num-components"));
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException, IndexException {
        // see PrefixMergePolicy.isMergeLagging() for the rationale behind this code.

        /**
         * case 1.
         * if totalImmutableCommponentCount < threshold,
         * merge operation is not lagged ==> return false.
         * case 2.
         * if a) totalImmutableCommponentCount >= threshold && b) there is an ongoing merge,
         * merge operation is lagged. ==> return true.
         * case 3. *SPECIAL CASE*
         * if a) totalImmutableCommponentCount >= threshold && b) there is *NO* ongoing merge,
         * merge operation is lagged. ==> *schedule a merge operation* and then return true.
         * This is a special case that requires to schedule a merge operation.
         * Otherwise, all flush operations will be hung.
         * This case can happen in a following situation:
         * The system may crash when
         * condition 1) the mergableImmutableCommponentCount >= threshold and
         * condition 2) merge operation is going on.
         * After the system is recovered, still condition 1) is true.
         * If there are flush operations in the same dataset partition after the recovery,
         * all these flush operations may not proceed since there is no ongoing merge and
         * there will be no new merge either in this situation.
         */

        List<ILSMComponent> immutableComponents = index.getImmutableComponents();
        int totalImmutableComponentCount = immutableComponents.size();

        // [case 1]
        if (totalImmutableComponentCount < numComponents) {
            return false;
        }

        boolean isMergeOngoing = isMergeOngoing(immutableComponents);

        // here, implicitly (totalImmutableComponentCount >= numComponents) is true by passing case 1.
        if (isMergeOngoing) {
            // [case 2]
            return true;
        } else {
            // [case 3]
            // schedule a merge operation after making sure that all components are mergable
            if (!areComponentsMergable(immutableComponents)) {
                throw new IllegalStateException();
            }
            ILSMIndexAccessor accessor = index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleMerge(index.getIOOperationCallback(), immutableComponents);
            return true;
        }
    }

    /**
     * checks whether all given components are mergable or not
     *
     * @param immutableComponents
     * @return true if all components are mergable, false otherwise.
     */
    private boolean areComponentsMergable(List<ILSMComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    /**
     * This method returns whether there is an ongoing merge operation or not by checking
     * each component state of given components.
     *
     * @param immutableComponents
     * @return true if there is an ongoing merge operation, false otherwise.
     */
    private boolean isMergeOngoing(List<ILSMComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }
}
