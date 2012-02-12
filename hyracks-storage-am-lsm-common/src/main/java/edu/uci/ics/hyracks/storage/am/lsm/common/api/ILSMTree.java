/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;

/**
 * Methods to be implemented by an LSM index, which are called from LSMHarness.
 * The implementations of the methods below should be thread agnostic.
 * Synchronization of LSM operations like updates/searches/flushes/merges are
 * done by the LSMHarness. For example, a flush() implementation should only
 * create and return the new on-disk component, ignoring the fact that
 * concurrent searches/updates/merges may be ongoing.
 * 
 */
public interface ILSMTree extends ITreeIndex {
    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException,
            TreeIndexException;

    public void search(ITreeIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred,
            IIndexOpContext ictx, boolean includeMemComponent, AtomicInteger searcherRefCount) throws HyracksDataException, TreeIndexException;

    public Object merge(List<Object> mergedComponents) throws HyracksDataException, TreeIndexException;

    public void addMergedComponent(Object newComponent, List<Object> mergedComponents);

    public void cleanUpAfterMerge(List<Object> mergedComponents) throws HyracksDataException;

    public Object flush() throws HyracksDataException, TreeIndexException;

    public void addFlushedComponent(Object index);

    public InMemoryFreePageManager getInMemoryFreePageManager();

    public void resetInMemoryComponent() throws HyracksDataException;

    public List<Object> getDiskComponents();
    
    public ILSMComponentFinalizer getComponentFinalizer();
}
