package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class LSMRTreeMergeOperation implements ILSMIOOperation {
    private final ILSMIndex index;
    private final List<ILSMComponent> mergingComponents;
    private final ITreeIndexCursor cursor;
    private final FileReference rtreeMergeTarget;
    private final FileReference btreeMergeTarget;
    private final ILSMIOOperationCallback callback;

    public LSMRTreeMergeOperation(ILSMIndex index, List<ILSMComponent> mergingComponents, ITreeIndexCursor cursor,
            FileReference rtreeMergeTarget, FileReference btreeMergeTarget, ILSMIOOperationCallback callback) {
        this.index = index;
        this.mergingComponents = mergingComponents;
        this.cursor = cursor;
        this.rtreeMergeTarget = rtreeMergeTarget;
        this.btreeMergeTarget = btreeMergeTarget;
        this.callback = callback;
    }

    @Override
    public List<IODeviceHandle> getReadDevices() {
        List<IODeviceHandle> devs = new ArrayList<IODeviceHandle>();
        for (ILSMComponent o : mergingComponents) {
            LSMRTreeComponent component = (LSMRTreeComponent) o;
            devs.add(component.getRTree().getFileReference().getDevideHandle());
        }
        return devs;
    }

    @Override
    public List<IODeviceHandle> getWriteDevices() {
        return Collections.singletonList(rtreeMergeTarget.getDevideHandle());
    }

    @Override
    public void perform() throws HyracksDataException, IndexException {
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        accessor.merge(this);
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    public FileReference getRTreeMergeTarget() {
        return rtreeMergeTarget;
    }

    public FileReference getBTreeMergeTarget() {
        return btreeMergeTarget;
    }

    public ITreeIndexCursor getCursor() {
        return cursor;
    }

    public List<ILSMComponent> getMergingComponents() {
        return mergingComponents;
    }

}
