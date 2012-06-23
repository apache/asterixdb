package edu.uci.ics.hyracks.test.support;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexArtifactMap;

public class TestIndexArtifactMap implements IIndexArtifactMap {

    private long counter = 0;
    private Map<String, Long> name2IdMap = new HashMap<String, Long>();

    @Override
    public long create(String baseDir, List<IODeviceHandle> IODeviceHandles) throws IOException {
        synchronized (name2IdMap) {
            for (IODeviceHandle dev : IODeviceHandles) {
                String fullDir = dev.getPath().toString() + "" + baseDir;
                if (name2IdMap.containsKey(fullDir)) {
                    throw new IOException();
                }
                name2IdMap.put(fullDir, counter++);
            }
        }
        return 0;
    }

    @Override
    public long get(String fullDir) {
        synchronized (name2IdMap) {
            return name2IdMap.get(fullDir);
        }
    }
}
