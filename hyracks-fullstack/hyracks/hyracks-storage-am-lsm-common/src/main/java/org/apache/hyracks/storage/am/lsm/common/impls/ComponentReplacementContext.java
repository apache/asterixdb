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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

public class ComponentReplacementContext implements ILSMIndexOperationContext {
    private static final Logger LOGGER = Logger.getLogger(ComponentReplacementContext.class.getName());
    private final List<ILSMComponent> components;
    private final List<ILSMComponent> diskComponents;
    private final List<ILSMComponentId> replacedComponentIds;
    private final int[] swapIndexes;
    private int count = 0;
    boolean accessingComponent = true;

    public ComponentReplacementContext(ILSMIndex lsmIndex) {
        components = new ArrayList<>(lsmIndex.getNumberOfAllMemoryComponents());
        replacedComponentIds = new ArrayList<>(lsmIndex.getNumberOfAllMemoryComponents());
        swapIndexes = new int[lsmIndex.getNumberOfAllMemoryComponents()];
        diskComponents = new ArrayList<>(lsmIndex.getNumberOfAllMemoryComponents());
    }

    @Override
    public void setOperation(IndexOperation newOp) throws HyracksDataException {
        // Do nothing
    }

    @Override
    public IndexOperation getOperation() {
        return IndexOperation.SEARCH;
    }

    @Override
    public void reset() {
        accessingComponent = true;
        components.clear();
        diskComponents.clear();
        replacedComponentIds.clear();
        count = 0;
    }

    @Override
    public List<ILSMComponent> getComponentHolder() {
        return components;
    }

    @Override
    public List<ILSMDiskComponent> getComponentsToBeMerged() {
        return Collections.emptyList();
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return null;
    }

    @Override
    public IModificationOperationCallback getModificationCallback() {
        return null;
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSearchPredicate(ISearchPredicate searchPredicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ISearchPredicate getSearchPredicate() {
        return null;
    }

    @Override
    public List<ILSMDiskComponent> getComponentsToBeReplicated() {
        return Collections.emptyList();
    }

    @Override
    public boolean isAccessingComponents() {
        return accessingComponent;
    }

    @Override
    public void setAccessingComponents(boolean accessingComponents) {
        // Ignore since this is only used for component replacement
    }

    @Override
    public PermutingTupleReference getIndexTuple() {
        return null;
    }

    @Override
    public PermutingTupleReference getFilterTuple() {
        return null;
    }

    @Override
    public MultiComparator getFilterCmp() {
        return null;
    }

    @Override
    public void logPerformanceCounters(int tupleCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void incrementEnterExitTime(long increment) {
        // Ignore since this is only used for component replacement
    }

    public boolean proceed(List<ILSMDiskComponent> allDiskComponents) throws HyracksDataException {
        for (int i = 0; i < components.size(); i++) {
            replacedComponentIds.add(components.get(i).getId());
            // ensure that disk component exists
            boolean found = false;
            LOGGER.log(Level.INFO, "Looking for a component with the id: " + replacedComponentIds.get(i));
            for (int j = 0; j < allDiskComponents.size(); j++) {
                ILSMDiskComponent dc = allDiskComponents.get(j);
                ILSMComponentId diskComponentId = dc.getId();
                LOGGER.log(Level.INFO, "Next disk component id: " + diskComponentId);
                if (diskComponentId.equals(replacedComponentIds.get(i))) {
                    found = true;
                    diskComponents.add(dc);
                    break;
                }
            }
            if (!found) {
                // component has been merged?
                LOGGER.log(Level.WARNING, "Memory Component with id = " + replacedComponentIds.get(i)
                        + " was flushed and merged before search cursor replaces it");
                return false;
            }
        }
        return true;
    }

    public void swapIndex(int i) {
        swapIndexes[count] = i;
        count++;
    }

    public void prepareToEnter() {
        components.clear();
        components.addAll(diskComponents);
        accessingComponent = false;
    }

    public void replace(ILSMIndexOperationContext ctx) {
        // Called after exit and enter has been completed
        try {
            for (int i = 0; i < count; i++) {
                ILSMComponent removed = ctx.getComponentHolder().remove(swapIndexes[i]);
                if (removed.getType() == LSMComponentType.MEMORY) {
                    LOGGER.log(Level.INFO, "Removed a memory component from the search operation");
                } else {
                    throw new IllegalStateException("Disk components can't be removed from the search operation");
                }
                ctx.getComponentHolder().add(swapIndexes[i], diskComponents.get(i));
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failure replacing memory components with disk components", e);
            throw e;
        }
    }

    @Override
    public ILSMIndex getIndex() {
        return null;
    }

    @Override
    public boolean isTracingEnabled() {
        return false;
    }
}
