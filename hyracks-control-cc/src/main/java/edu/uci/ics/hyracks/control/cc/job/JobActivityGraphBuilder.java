package edu.uci.ics.hyracks.control.cc.job;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class JobActivityGraphBuilder implements IActivityGraphBuilder {
    private static final Logger LOGGER = Logger.getLogger(JobActivityGraphBuilder.class.getName());

    private JobActivityGraph jag;

    @Override
    public void addBlockingEdge(IActivityNode blocker, IActivityNode blocked) {
        addToValueSet(jag.getBlocker2BlockedMap(), blocker.getActivityNodeId(), blocked.getActivityNodeId());
        addToValueSet(jag.getBlocked2BlockerMap(), blocked.getActivityNodeId(), blocker.getActivityNodeId());
    }

    @Override
    public void addSourceEdge(int operatorInputIndex, IActivityNode task, int taskInputIndex) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Adding source edge: " + task.getOwner().getOperatorId() + ":" + operatorInputIndex + " -> "
                    + task.getActivityNodeId() + ":" + taskInputIndex);
        }
        insertIntoIndexedMap(jag.getActivityInputMap(), task.getActivityNodeId(), taskInputIndex, operatorInputIndex);
        insertIntoIndexedMap(jag.getOperatorInputMap(), task.getOwner().getOperatorId(), operatorInputIndex,
                task.getActivityNodeId());
    }

    @Override
    public void addTargetEdge(int operatorOutputIndex, IActivityNode task, int taskOutputIndex) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Adding target edge: " + task.getOwner().getOperatorId() + ":" + operatorOutputIndex + " -> "
                    + task.getActivityNodeId() + ":" + taskOutputIndex);
        }
        insertIntoIndexedMap(jag.getActivityOutputMap(), task.getActivityNodeId(), taskOutputIndex, operatorOutputIndex);
        insertIntoIndexedMap(jag.getOperatorOutputMap(), task.getOwner().getOperatorId(), operatorOutputIndex,
                task.getActivityNodeId());
    }

    @Override
    public void addTask(IActivityNode task) {
        jag.getActivityNodeMap().put(task.getActivityNodeId(), task);
        addToValueSet(jag.getOperatorActivityMap(), task.getOwner().getOperatorId(), task.getActivityNodeId());
    }

    private <K, V> void addToValueSet(Map<K, Set<V>> map, K n1, V n2) {
        Set<V> targets = map.get(n1);
        if (targets == null) {
            targets = new HashSet<V>();
            map.put(n1, targets);
        }
        targets.add(n2);
    }

    private <T> void extend(List<T> list, int index) {
        int n = list.size();
        for (int i = n; i <= index; ++i) {
            list.add(null);
        }
    }

    public void init(String appName, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) {
        jag = new JobActivityGraph(appName, jobSpec, jobFlags);
    }

    private <K, V> void insertIntoIndexedMap(Map<K, List<V>> map, K key, int index, V value) {
        List<V> vList = map.get(key);
        if (vList == null) {
            vList = new ArrayList<V>();
            map.put(key, vList);
        }
        extend(vList, index);
        vList.set(index, value);
    }

    public JobActivityGraph getActivityGraph() {
        return jag;
    }
}