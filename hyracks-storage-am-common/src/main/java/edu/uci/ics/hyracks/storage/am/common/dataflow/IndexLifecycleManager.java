package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.common.file.IIndexArtifactMap;

public class IndexLifecycleManager implements IIndexLifecycleManager {

    private final IndexRegistry<IIndex> indexRegistry;
    private final Map<Long, Integer> refCountMap = new HashMap<Long, Integer>();

    public IndexLifecycleManager() {
        this.indexRegistry = new IndexRegistry<IIndex>();
    }

    @Override
    public void create(IIndexDataflowHelper helper) throws HyracksDataException {
        IHyracksTaskContext ctx = helper.getHyracksTaskContext();
        IIOManager ioManager = ctx.getIOManager();
        IIndexArtifactMap indexArtifactMap = helper.getOperatorDescriptor().getStorageManager()
                .getIndexArtifactMap(ctx);

        IIndex index;
        synchronized (indexRegistry) {
            long resourceID = helper.getResourceID();
            index = indexRegistry.get(resourceID);

            boolean generateResourceID = false;
            boolean register = false;
            if (index == null) {
                generateResourceID = true;
                register = true;
                index = helper.getIndexInstance();
            }

            index.create();

            if (generateResourceID) {
                try {
                    resourceID = indexArtifactMap.create(helper.getFileReference().getFile().getPath(),
                            ioManager.getIODevices());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            if (register) {
                indexRegistry.register(resourceID, index);
                refCountMap.put(resourceID, 0);
            }
        }
    }

    @Override
    public void destroy(IIndexDataflowHelper helper) throws HyracksDataException {
        IHyracksTaskContext ctx = helper.getHyracksTaskContext();
        IIOManager ioManager = ctx.getIOManager();
        IIndexArtifactMap indexArtifactMap = helper.getOperatorDescriptor().getStorageManager()
                .getIndexArtifactMap(ctx);

        IIndex index;
        long resourceID = helper.getResourceID();

        synchronized (indexRegistry) {
            index = indexRegistry.get(resourceID);
            if (index == null) {
                index = helper.getIndexInstance();
            }
            indexArtifactMap.delete(helper.getFileReference().getFile().getPath(), ioManager.getIODevices());
            indexRegistry.unregister(resourceID);
            index.destroy();
            refCountMap.remove(resourceID);
        }
    }

    @Override
    public IIndex open(IIndexDataflowHelper helper) throws HyracksDataException {
        IIndex index;
        long resourceID = helper.getResourceID();

        synchronized (indexRegistry) {
            index = indexRegistry.get(resourceID);

            boolean register = false;
            if (index == null) {
                refCountMap.put(resourceID, 1);
                register = true;
                index = helper.getIndexInstance();
            } else {
                int count = refCountMap.get(resourceID);
                refCountMap.put(resourceID, ++count);
            }
            index.activate();

            if (register) {
                indexRegistry.register(resourceID, index);
            }
        }
        return index;
    }

    @Override
    public void close(IIndexDataflowHelper helper) throws HyracksDataException {
        IIndex index;
        long resourceID = helper.getResourceID();

        synchronized (indexRegistry) {
            index = indexRegistry.get(resourceID);
            int count = refCountMap.get(resourceID);
            refCountMap.put(resourceID, --count);
            if (index == null) {
                throw new IllegalStateException("Trying to close an index that was not registered.");
            }

            if (count == 0) {
                indexRegistry.unregister(resourceID);
                index.deactivate();
            }
        }
    }

}
