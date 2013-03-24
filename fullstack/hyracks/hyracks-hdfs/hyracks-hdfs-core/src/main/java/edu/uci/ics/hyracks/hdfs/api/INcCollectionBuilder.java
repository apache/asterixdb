package edu.uci.ics.hyracks.hdfs.api;

import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.client.NodeControllerInfo;

/**
 * NC collections
 * 
 * @author yingyib
 */
public interface INcCollectionBuilder {

    public INcCollection build(Map<String, NodeControllerInfo> ncNameToNcInfos,
            Map<String, List<String>> ipToNcMapping, Map<String, Integer> ncNameToIndex, String[] NCs, int[] workloads,
            int slotLimit);
}
