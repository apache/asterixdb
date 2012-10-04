package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public interface ILSMIndexInternal extends ILSMIndex {

    public Object merge(List<Object> mergedComponents, ILSMIOOperation operation) throws HyracksDataException,
            IndexException;

    public void insertUpdateOrDelete(ITupleReference tuple, IIndexOperationContext ictx) throws HyracksDataException,
            IndexException;

    public void search(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred,
            IIndexOperationContext ictx, boolean includeMemComponent, AtomicInteger searcherRefCount)
            throws HyracksDataException, IndexException;

    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException,
            IndexException;

    public void addMergedComponent(Object newComponent, List<Object> mergedComponents);

    public void cleanUpAfterMerge(List<Object> mergedComponents) throws HyracksDataException;

    public Object flush(ILSMIOOperation operation) throws HyracksDataException, IndexException;

    public void addFlushedComponent(Object index);

    public IInMemoryFreePageManager getInMemoryFreePageManager();

    public void resetInMemoryComponent() throws HyracksDataException;

    public List<Object> getDiskComponents();

    public ILSMComponentFinalizer getComponentFinalizer();
}
