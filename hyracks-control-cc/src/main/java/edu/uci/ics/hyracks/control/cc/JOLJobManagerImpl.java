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
package edu.uci.ics.hyracks.control.cc;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import jol.core.Runtime;
import jol.types.basic.BasicTupleSet;
import jol.types.basic.Tuple;
import jol.types.basic.TupleSet;
import jol.types.exception.BadKeyException;
import jol.types.exception.UpdateException;
import jol.types.table.BasicTable;
import jol.types.table.EventTable;
import jol.types.table.Function;
import jol.types.table.Key;
import jol.types.table.TableName;
import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ChoiceLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionCountConstraint;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.Direction;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.statistics.JobStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageletStatistics;
import edu.uci.ics.hyracks.control.common.comm.Endpoint;
import edu.uci.ics.hyracks.control.common.job.JobPlan;

public class JOLJobManagerImpl implements IJobManager {
    private static final Logger LOGGER = Logger.getLogger(JOLJobManagerImpl.class.getName());

    public static final String JOL_SCOPE = "hyrackscc";

    private static final String SCHEDULER_OLG_FILE = "edu/uci/ics/hyracks/control/cc/scheduler.olg";

    private final Runtime jolRuntime;

    private final LinkedBlockingQueue<Runnable> jobQueue;

    private final JobTable jobTable;

    private final JobQueueThread jobQueueThread;

    private final OperatorDescriptorTable odTable;

    private final OperatorLocationTable olTable;

    private final OperatorCloneCountTable ocTable;

    private final ConnectorDescriptorTable cdTable;

    private final ActivityNodeTable anTable;

    private final ActivityConnectionTable acTable;

    private final ActivityBlockedTable abTable;

    private final JobStartTable jobStartTable;

    private final JobCleanUpTable jobCleanUpTable;

    private final JobCleanUpCompleteTable jobCleanUpCompleteTable;

    private final StartMessageTable startMessageTable;

    private final StageletCompleteTable stageletCompleteTable;

    private final StageletFailureTable stageletFailureTable;

    private final AvailableNodesTable availableNodesTable;

    private final RankedAvailableNodesTable rankedAvailableNodesTable;

    private final FailedNodesTable failedNodesTable;

    private final AbortMessageTable abortMessageTable;

    private final AbortNotifyTable abortNotifyTable;

    private final ExpandPartitionCountConstraintTableFunction expandPartitionCountConstraintFunction;

    private final List<String> rankedAvailableNodes;

    public JOLJobManagerImpl(final ClusterControllerService ccs, final Runtime jolRuntime) throws Exception {
        this.jolRuntime = jolRuntime;
        jobQueue = new LinkedBlockingQueue<Runnable>();
        jobQueueThread = new JobQueueThread();
        jobQueueThread.start();

        this.jobTable = new JobTable(jolRuntime);
        this.odTable = new OperatorDescriptorTable(jolRuntime);
        this.olTable = new OperatorLocationTable(jolRuntime);
        this.ocTable = new OperatorCloneCountTable(jolRuntime);
        this.cdTable = new ConnectorDescriptorTable(jolRuntime);
        this.anTable = new ActivityNodeTable(jolRuntime);
        this.acTable = new ActivityConnectionTable(jolRuntime);
        this.abTable = new ActivityBlockedTable(jolRuntime);
        this.jobStartTable = new JobStartTable();
        this.jobCleanUpTable = new JobCleanUpTable(jolRuntime);
        this.jobCleanUpCompleteTable = new JobCleanUpCompleteTable();
        this.startMessageTable = new StartMessageTable(jolRuntime);
        this.stageletCompleteTable = new StageletCompleteTable(jolRuntime);
        this.stageletFailureTable = new StageletFailureTable(jolRuntime);
        this.availableNodesTable = new AvailableNodesTable(jolRuntime);
        this.rankedAvailableNodesTable = new RankedAvailableNodesTable(jolRuntime);
        this.failedNodesTable = new FailedNodesTable(jolRuntime);
        this.abortMessageTable = new AbortMessageTable(jolRuntime);
        this.abortNotifyTable = new AbortNotifyTable(jolRuntime);
        this.expandPartitionCountConstraintFunction = new ExpandPartitionCountConstraintTableFunction();
        this.rankedAvailableNodes = new ArrayList<String>();

        jolRuntime.catalog().register(jobTable);
        jolRuntime.catalog().register(odTable);
        jolRuntime.catalog().register(olTable);
        jolRuntime.catalog().register(ocTable);
        jolRuntime.catalog().register(cdTable);
        jolRuntime.catalog().register(anTable);
        jolRuntime.catalog().register(acTable);
        jolRuntime.catalog().register(abTable);
        jolRuntime.catalog().register(jobStartTable);
        jolRuntime.catalog().register(jobCleanUpTable);
        jolRuntime.catalog().register(jobCleanUpCompleteTable);
        jolRuntime.catalog().register(startMessageTable);
        jolRuntime.catalog().register(stageletCompleteTable);
        jolRuntime.catalog().register(stageletFailureTable);
        jolRuntime.catalog().register(availableNodesTable);
        jolRuntime.catalog().register(rankedAvailableNodesTable);
        jolRuntime.catalog().register(failedNodesTable);
        jolRuntime.catalog().register(abortMessageTable);
        jolRuntime.catalog().register(abortNotifyTable);
        jolRuntime.catalog().register(expandPartitionCountConstraintFunction);

        jobTable.register(new JobTable.Callback() {
            @Override
            public void deletion(TupleSet arg0) {
                jobTable.notifyAll();
            }

            @Override
            public void insertion(TupleSet arg0) {
                jobTable.notifyAll();
            }
        });

        startMessageTable.register(new StartMessageTable.Callback() {
            @Override
            public void deletion(TupleSet tuples) {

            }

            @SuppressWarnings("unchecked")
            @Override
            public void insertion(TupleSet tuples) {
                for (final Tuple t : tuples) {
                    jobQueue.add(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Object[] data = t.toArray();
                                UUID jobId = (UUID) data[0];
                                UUID stageId = (UUID) data[1];
                                Integer attempt = (Integer) data[2];
                                JobPlan plan = (JobPlan) data[3];
                                Set<List> ts = (Set<List>) data[4];
                                Map<OperatorDescriptorId, Set<Integer>> opPartitions = new HashMap<OperatorDescriptorId, Set<Integer>>();
                                for (List t2 : ts) {
                                    Object[] t2Data = t2.toArray();
                                    Set<List> activityInfoSet = (Set<List>) t2Data[1];
                                    for (List l : activityInfoSet) {
                                        Object[] lData = l.toArray();
                                        ActivityNodeId aid = (ActivityNodeId) lData[0];
                                        Set<Integer> opParts = opPartitions.get(aid.getOperatorDescriptorId());
                                        if (opParts == null) {
                                            opParts = new HashSet<Integer>();
                                            opPartitions.put(aid.getOperatorDescriptorId(), opParts);
                                        }
                                        opParts.add((Integer) lData[1]);
                                    }
                                }
                                ClusterControllerService.Phase1Installer[] p1is = new ClusterControllerService.Phase1Installer[ts
                                        .size()];
                                int i = 0;
                                for (List t2 : ts) {
                                    Object[] t2Data = t2.toArray();
                                    Map<ActivityNodeId, Set<Integer>> tasks = new HashMap<ActivityNodeId, Set<Integer>>();
                                    Set<List> activityInfoSet = (Set<List>) t2Data[1];
                                    for (List l : activityInfoSet) {
                                        Object[] lData = l.toArray();
                                        ActivityNodeId aid = (ActivityNodeId) lData[0];
                                        Set<Integer> aParts = tasks.get(aid);
                                        if (aParts == null) {
                                            aParts = new HashSet<Integer>();
                                            tasks.put(aid, aParts);
                                        }
                                        aParts.add((Integer) lData[1]);
                                    }
                                    p1is[i++] = new ClusterControllerService.Phase1Installer((String) t2Data[0], jobId,
                                            plan, stageId, attempt, tasks, opPartitions);
                                }
                                LOGGER.info("Stage start - Phase 1");
                                Map<PortInstanceId, Endpoint> globalPortMap = ccs.runRemote(p1is,
                                        new ClusterControllerService.PortMapMergingAccumulator());

                                ClusterControllerService.Phase2Installer[] p2is = new ClusterControllerService.Phase2Installer[ts
                                        .size()];
                                ClusterControllerService.Phase3Installer[] p3is = new ClusterControllerService.Phase3Installer[ts
                                        .size()];
                                ClusterControllerService.StageStarter[] ss = new ClusterControllerService.StageStarter[ts
                                        .size()];
                                i = 0;
                                for (List t2 : ts) {
                                    Object[] t2Data = t2.toArray();
                                    Map<ActivityNodeId, Set<Integer>> tasks = new HashMap<ActivityNodeId, Set<Integer>>();
                                    Set<List> activityInfoSet = (Set<List>) t2Data[1];
                                    for (List l : activityInfoSet) {
                                        Object[] lData = l.toArray();
                                        ActivityNodeId aid = (ActivityNodeId) lData[0];
                                        Set<Integer> aParts = tasks.get(aid);
                                        if (aParts == null) {
                                            aParts = new HashSet<Integer>();
                                            tasks.put(aid, aParts);
                                        }
                                        aParts.add((Integer) lData[1]);
                                    }
                                    p2is[i] = new ClusterControllerService.Phase2Installer((String) t2Data[0], jobId,
                                            plan, stageId, tasks, opPartitions, globalPortMap);
                                    p3is[i] = new ClusterControllerService.Phase3Installer((String) t2Data[0], jobId,
                                            stageId);
                                    ss[i] = new ClusterControllerService.StageStarter((String) t2Data[0], jobId,
                                            stageId);
                                    ++i;
                                }
                                LOGGER.info("Stage start - Phase 2");
                                ccs.runRemote(p2is, null);
                                LOGGER.info("Stage start - Phase 3");
                                ccs.runRemote(p3is, null);
                                LOGGER.info("Stage start");
                                ccs.runRemote(ss, null);
                                LOGGER.info("Stage started");
                            } catch (Exception e) {
                            }
                        }
                    });
                }
            }
        });

        jobCleanUpTable.register(new JobCleanUpTable.Callback() {
            @Override
            public void deletion(TupleSet tuples) {
            }

            @SuppressWarnings("unchecked")
            @Override
            public void insertion(TupleSet tuples) {
                for (final Tuple t : tuples) {
                    jobQueue.add(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Object[] data = t.toArray();
                                UUID jobId = (UUID) data[0];
                                Set<String> ts = (Set<String>) data[1];
                                ClusterControllerService.JobCompleteNotifier[] jcns = new ClusterControllerService.JobCompleteNotifier[ts
                                        .size()];
                                int i = 0;
                                for (String n : ts) {
                                    jcns[i++] = new ClusterControllerService.JobCompleteNotifier(n, jobId);
                                }
                                try {
                                    ccs.runRemote(jcns, null);
                                } finally {
                                    BasicTupleSet jccTuples = new BasicTupleSet(JobCleanUpCompleteTable
                                            .createTuple(jobId));
                                    jolRuntime.schedule(JOL_SCOPE, JobCleanUpCompleteTable.TABLE_NAME, jccTuples, null);
                                    jolRuntime.evaluate();
                                }
                            } catch (Exception e) {
                            }
                        }
                    });
                }
            }
        });

        abortMessageTable.register(new AbortMessageTable.Callback() {
            @Override
            public void deletion(TupleSet tuples) {

            }

            @SuppressWarnings("unchecked")
            @Override
            public void insertion(TupleSet tuples) {
                for (final Tuple t : tuples) {
                    jobQueue.add(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Object[] data = t.toArray();
                                UUID jobId = (UUID) data[0];
                                UUID stageId = (UUID) data[1];
                                Integer attempt = (Integer) data[2];
                                Set<List> ts = (Set<List>) data[4];
                                ClusterControllerService.JobletAborter[] jas = new ClusterControllerService.JobletAborter[ts
                                        .size()];
                                int i = 0;
                                BasicTupleSet notificationTuples = new BasicTupleSet();
                                for (List t2 : ts) {
                                    Object[] t2Data = t2.toArray();
                                    String nodeId = (String) t2Data[0];
                                    jas[i++] = new ClusterControllerService.JobletAborter(nodeId, jobId, stageId,
                                            attempt);
                                    notificationTuples.add(AbortNotifyTable
                                            .createTuple(jobId, stageId, nodeId, attempt));
                                }
                                try {
                                    ccs.runRemote(jas, null);
                                } finally {
                                    jolRuntime.schedule(JOL_SCOPE, AbortNotifyTable.TABLE_NAME, notificationTuples,
                                            null);
                                    jolRuntime.evaluate();
                                }
                            } catch (Exception e) {
                            }
                        }
                    });
                }
            }
        });

        jolRuntime.install(JOL_SCOPE, ClassLoader.getSystemResource(SCHEDULER_OLG_FILE));
        jolRuntime.evaluate();
    }

    @Override
    public UUID createJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        final UUID jobId = UUID.randomUUID();

        final JobPlanBuilder builder = new JobPlanBuilder();
        builder.init(jobSpec, jobFlags);

        final BasicTupleSet anTuples = new BasicTupleSet();
        final BasicTupleSet acTuples = new BasicTupleSet();
        final BasicTupleSet abTuples = new BasicTupleSet();
        IActivityGraphBuilder gBuilder = new IActivityGraphBuilder() {
            @Override
            public void addTask(IActivityNode task) {
                anTuples.add(ActivityNodeTable.createTuple(jobId, task));
                builder.addTask(task);
            }

            @Override
            public void addTargetEdge(int operatorOutputIndex, IActivityNode task, int taskOutputIndex) {
                acTuples.add(ActivityConnectionTable.createTuple(jobId, task, Direction.OUTPUT, operatorOutputIndex,
                        taskOutputIndex));
                builder.addTargetEdge(operatorOutputIndex, task, taskOutputIndex);
            }

            @Override
            public void addSourceEdge(int operatorInputIndex, IActivityNode task, int taskInputIndex) {
                acTuples.add(ActivityConnectionTable.createTuple(jobId, task, Direction.INPUT, operatorInputIndex,
                        taskInputIndex));
                builder.addSourceEdge(operatorInputIndex, task, taskInputIndex);
            }

            @Override
            public void addBlockingEdge(IActivityNode blocker, IActivityNode blocked) {
                abTuples.add(ActivityBlockedTable.createTuple(jobId, blocker, blocked));
                builder.addBlockingEdge(blocker, blocked);
            }
        };

        BasicTupleSet odTuples = new BasicTupleSet();
        BasicTupleSet olTuples = new BasicTupleSet();
        BasicTupleSet ocTuples = new BasicTupleSet();
        for (Map.Entry<OperatorDescriptorId, IOperatorDescriptor> e : jobSpec.getOperatorMap().entrySet()) {
            IOperatorDescriptor od = e.getValue();
            int nPartitions = addPartitionConstraintTuples(jobId, od, olTuples, ocTuples);
            odTuples.add(OperatorDescriptorTable.createTuple(jobId, nPartitions, od));
            od.contributeTaskGraph(gBuilder);
        }

        BasicTupleSet cdTuples = new BasicTupleSet();
        for (Map.Entry<ConnectorDescriptorId, IConnectorDescriptor> e : jobSpec.getConnectorMap().entrySet()) {
            cdTuples.add(ConnectorDescriptorTable.createTuple(jobId, jobSpec, e.getValue()));
        }

        BasicTupleSet jobTuples = new BasicTupleSet(JobTable.createInitialJobTuple(jobId, jobSpec, builder.getPlan()));

        jolRuntime.schedule(JOL_SCOPE, JobTable.TABLE_NAME, jobTuples, null);
        jolRuntime.schedule(JOL_SCOPE, OperatorDescriptorTable.TABLE_NAME, odTuples, null);
        jolRuntime.schedule(JOL_SCOPE, OperatorLocationTable.TABLE_NAME, olTuples, null);
        jolRuntime.schedule(JOL_SCOPE, OperatorCloneCountTable.TABLE_NAME, ocTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ConnectorDescriptorTable.TABLE_NAME, cdTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ActivityNodeTable.TABLE_NAME, anTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ActivityConnectionTable.TABLE_NAME, acTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ActivityBlockedTable.TABLE_NAME, abTuples, null);

        jolRuntime.evaluate();

        return jobId;
    }

    private int addPartitionConstraintTuples(UUID jobId, IOperatorDescriptor od, BasicTupleSet olTuples,
            BasicTupleSet ocTuples) {
        PartitionConstraint pc = od.getPartitionConstraint();

        switch (pc.getPartitionConstraintType()) {
            case COUNT:
                int count = ((PartitionCountConstraint) pc).getCount();
                ocTuples.add(OperatorCloneCountTable.createTuple(jobId, od.getOperatorId(), count));
                return count;

            case EXPLICIT:
                LocationConstraint[] locationConstraints = ((ExplicitPartitionConstraint) pc).getLocationConstraints();
                for (int i = 0; i < locationConstraints.length; ++i) {
                    addLocationConstraintTuple(olTuples, jobId, od.getOperatorId(), i, locationConstraints[i], 0);
                }
                return locationConstraints.length;
        }
        throw new IllegalArgumentException();
    }

    private void addLocationConstraintTuple(BasicTupleSet olTuples, UUID jobId, OperatorDescriptorId opId, int i,
            LocationConstraint locationConstraint, int benefit) {
        switch (locationConstraint.getConstraintType()) {
            case ABSOLUTE:
                String nodeId = ((AbsoluteLocationConstraint) locationConstraint).getLocationId();
                olTuples.add(OperatorLocationTable.createTuple(jobId, opId, nodeId, i, benefit));
                break;

            case CHOICE:
                int index = 0;
                for (LocationConstraint lc : ((ChoiceLocationConstraint) locationConstraint).getChoices()) {
                    addLocationConstraintTuple(olTuples, jobId, opId, i, lc, benefit - index);
                    index++;
                }
        }
    }

    @Override
    public JobStatus getJobStatus(UUID jobId) {
        synchronized (jobTable) {
            try {
                Tuple jobTuple = jobTable.lookupJob(jobId);
                if (jobTuple == null) {
                    return null;
                }
                return (JobStatus) jobTuple.value(1);
            } catch (BadKeyException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public synchronized void notifyNodeFailure(String nodeId) throws Exception {
        int len = rankedAvailableNodes.size();
        int delIndex = -1;
        for (int i = 0; i < len; ++i) {
            if (nodeId.equals(rankedAvailableNodes.get(i))) {
                delIndex = i;
                break;
            }
        }
        if (delIndex < 0) {
            return;
        }
        BasicTupleSet delRANTuples = new BasicTupleSet();
        delRANTuples.add(RankedAvailableNodesTable.createTuple(nodeId, delIndex));

        BasicTupleSet insRANTuples = new BasicTupleSet();
        for (int i = delIndex + 1; i < len; ++i) {
            insRANTuples.add(RankedAvailableNodesTable.createTuple(rankedAvailableNodes.get(i), i - 1));
        }

        rankedAvailableNodes.remove(delIndex);

        jolRuntime.schedule(JOL_SCOPE, RankedAvailableNodesTable.TABLE_NAME, insRANTuples, delRANTuples);

        BasicTupleSet unavailableTuples = new BasicTupleSet(AvailableNodesTable.createTuple(nodeId));

        jolRuntime.schedule(JOL_SCOPE, AvailableNodesTable.TABLE_NAME, null, unavailableTuples);

        jolRuntime.evaluate();

        BasicTupleSet failedTuples = new BasicTupleSet(FailedNodesTable.createTuple(nodeId));

        jolRuntime.schedule(JOL_SCOPE, FailedNodesTable.TABLE_NAME, failedTuples, null);

        jolRuntime.evaluate();
    }

    @Override
    public synchronized void notifyStageletComplete(UUID jobId, UUID stageId, int attempt, String nodeId,
            StageletStatistics statistics) throws Exception {
        BasicTupleSet scTuples = new BasicTupleSet();
        scTuples.add(StageletCompleteTable.createTuple(jobId, stageId, nodeId, attempt, statistics));

        jolRuntime.schedule(JOL_SCOPE, StageletCompleteTable.TABLE_NAME, scTuples, null);

        jolRuntime.evaluate();
    }

    @Override
    public synchronized void notifyStageletFailure(UUID jobId, UUID stageId, int attempt, String nodeId)
            throws Exception {
        BasicTupleSet sfTuples = new BasicTupleSet();
        sfTuples.add(StageletFailureTable.createTuple(jobId, stageId, nodeId, attempt));

        jolRuntime.schedule(JOL_SCOPE, StageletFailureTable.TABLE_NAME, sfTuples, null);

        jolRuntime.evaluate();
    }

    @Override
    public void start(UUID jobId) throws Exception {
        BasicTupleSet jsTuples = new BasicTupleSet();
        jsTuples.add(JobStartTable.createTuple(jobId, System.currentTimeMillis()));

        jolRuntime.schedule(JOL_SCOPE, JobStartTable.TABLE_NAME, jsTuples, null);

        jolRuntime.evaluate();
    }

    @Override
    public synchronized void registerNode(String nodeId) throws Exception {
        rankedAvailableNodes.add(nodeId);
        BasicTupleSet insRANTuples = new BasicTupleSet();
        insRANTuples.add(RankedAvailableNodesTable.createTuple(nodeId, rankedAvailableNodes.size() - 1));

        jolRuntime.schedule(JOL_SCOPE, RankedAvailableNodesTable.TABLE_NAME, insRANTuples, null);

        BasicTupleSet availableTuples = new BasicTupleSet(AvailableNodesTable.createTuple(nodeId));

        jolRuntime.schedule(JOL_SCOPE, AvailableNodesTable.TABLE_NAME, availableTuples, null);

        jolRuntime.evaluate();

        BasicTupleSet unfailedTuples = new BasicTupleSet(FailedNodesTable.createTuple(nodeId));

        jolRuntime.schedule(JOL_SCOPE, FailedNodesTable.TABLE_NAME, null, unfailedTuples);

        jolRuntime.evaluate();
    }

    @Override
    public JobStatistics waitForCompletion(UUID jobId) throws Exception {
        synchronized (jobTable) {
            Tuple jobTuple = null;
            while ((jobTuple = jobTable.lookupJob(jobId)) != null && jobTuple.value(1) != JobStatus.TERMINATED) {
                jobTable.wait();
            }
            return jobTuple == null ? null : jobTable.buildJobStatistics(jobTuple);
        }
    }

    /*
     * declare(job, keys(0), {JobId, Status, JobSpec, JobPlan})
     */
    private static class JobTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "job");

        private static Key PRIMARY_KEY = new Key(0);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, JobStatus.class, JobSpecification.class,
                JobPlan.class, Set.class };

        public JobTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        @SuppressWarnings("unchecked")
        static Tuple createInitialJobTuple(UUID jobId, JobSpecification jobSpec, JobPlan plan) {
            return new Tuple(jobId, JobStatus.INITIALIZED, jobSpec, plan, new HashSet());
        }

        @SuppressWarnings("unchecked")
        JobStatistics buildJobStatistics(Tuple jobTuple) {
            Set<Set<StageletStatistics>> statsSet = (Set<Set<StageletStatistics>>) jobTuple.value(4);
            JobStatistics stats = new JobStatistics();
            if (statsSet != null) {
                for (Set<StageletStatistics> stageStatsSet : statsSet) {
                    StageStatistics stageStats = new StageStatistics();
                    for (StageletStatistics stageletStats : stageStatsSet) {
                        stageStats.addStageletStatistics(stageletStats);
                    }
                    stats.addStageStatistics(stageStats);
                }
            }
            return stats;
        }

        Tuple lookupJob(UUID jobId) throws BadKeyException {
            TupleSet set = primary().lookupByKey(jobId);
            if (set.isEmpty()) {
                return null;
            }
            return (Tuple) set.toArray()[0];
        }
    }

    /*
     * declare(operatordescriptor, keys(0, 1), {JobId, ODId, OperatorDescriptor})
     */
    private static class OperatorDescriptorTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "operatordescriptor");

        private static Key PRIMARY_KEY = new Key(0, 1);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, OperatorDescriptorId.class, Integer.class,
                IOperatorDescriptor.class };

        public OperatorDescriptorTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, int nPartitions, IOperatorDescriptor od) {
            return new Tuple(jobId, od.getOperatorId(), nPartitions, od);
        }
    }

    /*
     * declare(operatorlocation, keys(0, 1), {JobId, ODId, NodeId})
     */
    private static class OperatorLocationTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "operatorlocation");

        private static Key PRIMARY_KEY = new Key();

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, OperatorDescriptorId.class, String.class,
                Integer.class, Integer.class };

        public OperatorLocationTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, OperatorDescriptorId opId, String nodeId, int partition, int benefit) {
            return new Tuple(jobId, opId, nodeId, partition, benefit);
        }
    }

    /*
     * declare(operatorclonecount, keys(0, 1), {JobId, ODId, Count})
     */
    private static class OperatorCloneCountTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "operatorclonecount");

        private static Key PRIMARY_KEY = new Key();

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, OperatorDescriptorId.class, Integer.class };

        public OperatorCloneCountTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, OperatorDescriptorId opId, int cloneCount) {
            return new Tuple(jobId, opId, cloneCount);
        }
    }

    /*
     * declare(connectordescriptor, keys(0, 1), {JobId, CDId, SrcODId, SrcPort, DestODId, DestPort, ConnectorDescriptor})
     */
    private static class ConnectorDescriptorTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "connectordescriptor");

        private static Key PRIMARY_KEY = new Key(0, 1);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, ConnectorDescriptorId.class,
                OperatorDescriptorId.class, Integer.class, OperatorDescriptorId.class, Integer.class,
                IConnectorDescriptor.class };

        public ConnectorDescriptorTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, JobSpecification jobSpec, IConnectorDescriptor conn) {
            IOperatorDescriptor srcOD = jobSpec.getProducer(conn);
            int srcPort = jobSpec.getProducerOutputIndex(conn);
            IOperatorDescriptor destOD = jobSpec.getConsumer(conn);
            int destPort = jobSpec.getConsumerInputIndex(conn);
            Tuple cdTuple = new Tuple(jobId, conn.getConnectorId(), srcOD.getOperatorId(), srcPort,
                    destOD.getOperatorId(), destPort, conn);
            return cdTuple;
        }
    }

    /*
     * declare(activitynode, keys(0, 1, 2), {JobId, OperatorId, ActivityId, ActivityNode})
     */
    private static class ActivityNodeTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "activitynode");

        private static Key PRIMARY_KEY = new Key(0, 1, 2);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, OperatorDescriptorId.class,
                ActivityNodeId.class, IActivityNode.class };

        public ActivityNodeTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, IActivityNode aNode) {
            return new Tuple(jobId, aNode.getActivityNodeId().getOperatorDescriptorId(), aNode.getActivityNodeId(),
                    aNode);
        }
    }

    /*
     * declare(activityconnection, keys(0, 1, 2, 3), {JobId, OperatorId, Integer, Direction, ActivityNodeId, Integer})
     */
    private static class ActivityConnectionTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "activityconnection");

        private static Key PRIMARY_KEY = new Key(0, 1, 2, 3);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, OperatorDescriptorId.class, Integer.class,
                Direction.class, ActivityNodeId.class, Integer.class };

        public ActivityConnectionTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, IActivityNode aNode, Direction direction, int odPort, int activityPort) {
            return new Tuple(jobId, aNode.getActivityNodeId().getOperatorDescriptorId(), odPort, direction,
                    aNode.getActivityNodeId(), activityPort);
        }
    }

    /*
     * declare(activityblocked, keys(0, 1, 2, 3), {JobId, OperatorId, BlockerActivityId, BlockedActivityId})
     */
    private static class ActivityBlockedTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "activityblocked");

        private static Key PRIMARY_KEY = new Key(0, 1, 2, 3, 4);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, OperatorDescriptorId.class,
                ActivityNodeId.class, OperatorDescriptorId.class, ActivityNodeId.class };

        public ActivityBlockedTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, IActivityNode blocker, IActivityNode blocked) {
            ActivityNodeId blockerANId = blocker.getActivityNodeId();
            OperatorDescriptorId blockerODId = blockerANId.getOperatorDescriptorId();
            ActivityNodeId blockedANId = blocked.getActivityNodeId();
            OperatorDescriptorId blockedODId = blockedANId.getOperatorDescriptorId();
            return new Tuple(jobId, blockerODId, blockerANId, blockedODId, blockedANId);
        }
    }

    /*
     * declare(jobstart, keys(0), {JobId, SubmitTime})
     */
    private static class JobStartTable extends EventTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "jobstart");

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, Long.class };

        public JobStartTable() {
            super(TABLE_NAME, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, long submitTime) {
            return new Tuple(jobId, submitTime);
        }
    }

    /*
     * declare(startmessage, keys(0, 1), {JobId, StageId, JobPlan, TupleSet})
     */
    private static class StartMessageTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "startmessage");

        private static Key PRIMARY_KEY = new Key(0, 1, 2);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, UUID.class, Integer.class, JobPlan.class,
                Set.class };

        public StartMessageTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }
    }

    /*
     * declare(jobcleanup, keys(0), {JobId, Set<NodeId>})
     */
    private static class JobCleanUpTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "jobcleanup");

        private static Key PRIMARY_KEY = new Key(0);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, Set.class };

        public JobCleanUpTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }
    }

    /*
     * declare(jobcleanupcomplete, keys(0), {JobId})
     */
    private static class JobCleanUpCompleteTable extends EventTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "jobcleanupcomplete");

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class };

        public JobCleanUpCompleteTable() {
            super(TABLE_NAME, SCHEMA);
        }

        public static Tuple createTuple(UUID jobId) {
            return new Tuple(jobId);
        }
    }

    /*
     * declare(stageletcomplete, keys(0, 1, 2, 3), {JobId, StageId, NodeId, Attempt, StageletStatistics})
     */
    private static class StageletCompleteTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "stageletcomplete");

        private static Key PRIMARY_KEY = new Key(0, 1, 2, 3);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, UUID.class, String.class, Integer.class,
                StageletStatistics.class };

        public StageletCompleteTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        public static Tuple createTuple(UUID jobId, UUID stageId, String nodeId, int attempt,
                StageletStatistics statistics) {
            return new Tuple(jobId, stageId, nodeId, attempt, statistics);
        }
    }

    /*
     * declare(stageletfailure, keys(0, 1, 2, 3), {JobId, StageId, NodeId, Attempt})
     */
    private static class StageletFailureTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "stageletfailure");

        private static Key PRIMARY_KEY = new Key(0, 1, 2, 3);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, UUID.class, String.class, Integer.class };

        public StageletFailureTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        public static Tuple createTuple(UUID jobId, UUID stageId, String nodeId, int attempt) {
            return new Tuple(jobId, stageId, nodeId, attempt);
        }
    }

    /*
     * declare(availablenodes, keys(0), {NodeId})
     */
    private static class AvailableNodesTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "availablenodes");

        private static Key PRIMARY_KEY = new Key(0);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { String.class };

        public AvailableNodesTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        public static Tuple createTuple(String nodeId) {
            return new Tuple(nodeId);
        }
    }

    /*
     * declare(rankedavailablenodes, keys(0), {NodeId, Integer})
     */
    private static class RankedAvailableNodesTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "rankedavailablenodes");

        private static Key PRIMARY_KEY = new Key(0);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { String.class, Integer.class };

        public RankedAvailableNodesTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        public static Tuple createTuple(String nodeId, int rank) {
            return new Tuple(nodeId, rank);
        }
    }

    /*
     * declare(failednodes, keys(0), {NodeId})
     */
    private static class FailedNodesTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "failednodes");

        private static Key PRIMARY_KEY = new Key(0);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { String.class };

        public FailedNodesTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        public static Tuple createTuple(String nodeId) {
            return new Tuple(nodeId);
        }
    }

    /*
     * declare(abortmessage, keys(0, 1), {JobId, StageId, Attempt, JobPlan, TupleSet})
     */
    private static class AbortMessageTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "abortmessage");

        private static Key PRIMARY_KEY = new Key(0, 1, 2);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, UUID.class, Integer.class, JobPlan.class,
                Set.class };

        public AbortMessageTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }
    }

    /*
     * declare(abortnotify, keys(0, 1, 2, 3), {JobId, StageId, NodeId, Attempt, StageletStatistics})
     */
    private static class AbortNotifyTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "abortnotify");

        private static Key PRIMARY_KEY = new Key(0, 1, 2, 3);

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, UUID.class, String.class, Integer.class };

        public AbortNotifyTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        public static Tuple createTuple(UUID jobId, UUID stageId, String nodeId, int attempt) {
            return new Tuple(jobId, stageId, nodeId, attempt);
        }
    }

    private static class ExpandPartitionCountConstraintTableFunction extends Function {
        private static final String TABLE_NAME = "expandpartitioncountconstraint";

        @SuppressWarnings("unchecked")
        private static final Class[] SCHEMA = new Class[] { UUID.class, OperatorDescriptorId.class, Integer.class,
                Integer.class };

        public ExpandPartitionCountConstraintTableFunction() {
            super(TABLE_NAME, SCHEMA);
        }

        @Override
        public TupleSet insert(TupleSet tuples, TupleSet conflicts) throws UpdateException {
            TupleSet result = new BasicTupleSet();
            int counter = 0;
            for (Tuple t : tuples) {
                int nPartitions = (Integer) t.value(2);
                for (int i = 0; i < nPartitions; ++i) {
                    result.add(new Tuple(t.value(0), t.value(1), i, counter++));
                }
            }
            return result;
        }
    }

    private class JobQueueThread extends Thread {
        public JobQueueThread() {
            setDaemon(true);
        }

        public void run() {
            Runnable r;
            while (true) {
                try {
                    r = jobQueue.take();
                } catch (InterruptedException e) {
                    continue;
                }
                try {
                    r.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}