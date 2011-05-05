package edu.uci.ics.hyracks.control.cc.job;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class JobActivityGraphBuilder implements IActivityGraphBuilder {
    private static final Logger LOGGER = Logger.getLogger(JobActivityGraphBuilder.class.getName());

    private JobActivityGraph jag;

    @Override
    public void addBlockingEdge(IActivity blocker, IActivity blocked) {
        addToValueSet(jag.getBlocker2BlockedMap(), blocker.getActivityId(), blocked.getActivityId());
        addToValueSet(jag.getBlocked2BlockerMap(), blocked.getActivityId(), blocker.getActivityId());
    }

    @Override
    public void addSourceEdge(int operatorInputIndex, IActivity task, int taskInputIndex) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Adding source edge: " + task.getActivityId().getOperatorDescriptorId() + ":"
                    + operatorInputIndex + " -> " + task.getActivityId() + ":" + taskInputIndex);
        }
        insertIntoIndexedMap(jag.getActivityInputMap(), task.getActivityId(), taskInputIndex, operatorInputIndex);
        insertIntoIndexedMap(jag.getOperatorInputMap(), task.getActivityId().getOperatorDescriptorId(),
                operatorInputIndex, task.getActivityId());
    }

    @Override
    public void addTargetEdge(int operatorOutputIndex, IActivity task, int taskOutputIndex) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Adding target edge: " + task.getActivityId().getOperatorDescriptorId() + ":"
                    + operatorOutputIndex + " -> " + task.getActivityId() + ":" + taskOutputIndex);
        }
        insertIntoIndexedMap(jag.getActivityOutputMap(), task.getActivityId(), taskOutputIndex, operatorOutputIndex);
        insertIntoIndexedMap(jag.getOperatorOutputMap(), task.getActivityId().getOperatorDescriptorId(),
                operatorOutputIndex, task.getActivityId());
    }

    @Override
    public void addActivity(IActivity task) {
        ActivityId activityId = task.getActivityId();
        jag.getActivityNodeMap().put(activityId, task);
        addToValueSet(jag.getOperatorActivityMap(), activityId.getOperatorDescriptorId(), activityId);
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