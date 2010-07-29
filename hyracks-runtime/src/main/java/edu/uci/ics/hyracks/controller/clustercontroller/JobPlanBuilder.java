package edu.uci.ics.hyracks.controller.clustercontroller;

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
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.job.JobPlan;

public class JobPlanBuilder implements IActivityGraphBuilder {
    private static final Logger LOGGER = Logger.getLogger(JobPlanBuilder.class.getName());

    private JobPlan plan;

    @Override
    public void addBlockingEdge(IActivityNode blocker, IActivityNode blocked) {
        addToValueSet(plan.getBlocker2BlockedMap(), blocker.getActivityNodeId(), blocked.getActivityNodeId());
        addToValueSet(plan.getBlocked2BlockerMap(), blocked.getActivityNodeId(), blocker.getActivityNodeId());
    }

    @Override
    public void addSourceEdge(int operatorInputIndex, IActivityNode task, int taskInputIndex) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Adding source edge: " + task.getOwner().getOperatorId() + ":" + operatorInputIndex + " -> "
                + task.getActivityNodeId() + ":" + taskInputIndex);
        }
        insertIntoIndexedMap(plan.getTaskInputMap(), task.getActivityNodeId(), taskInputIndex, operatorInputIndex);
        insertIntoIndexedMap(plan.getOperatorInputMap(), task.getOwner().getOperatorId(), operatorInputIndex, task
            .getActivityNodeId());
    }

    @Override
    public void addTargetEdge(int operatorOutputIndex, IActivityNode task, int taskOutputIndex) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Adding target edge: " + task.getOwner().getOperatorId() + ":" + operatorOutputIndex + " -> "
                + task.getActivityNodeId() + ":" + taskOutputIndex);
        }
        insertIntoIndexedMap(plan.getTaskOutputMap(), task.getActivityNodeId(), taskOutputIndex, operatorOutputIndex);
        insertIntoIndexedMap(plan.getOperatorOutputMap(), task.getOwner().getOperatorId(), operatorOutputIndex, task
            .getActivityNodeId());
    }

    @Override
    public void addTask(IActivityNode task) {
        plan.getActivityNodeMap().put(task.getActivityNodeId(), task);
        addToValueSet(plan.getOperatorTaskMap(), task.getOwner().getOperatorId(), task.getActivityNodeId());
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

    public void init(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) {
        plan = new JobPlan(jobSpec, jobFlags);
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

    public JobPlan getPlan() {
        return plan;
    }
}