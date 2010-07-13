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
package edu.uci.ics.hyracks.controller.clustercontroller;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import jol.core.Runtime;
import jol.types.basic.BasicTupleSet;
import jol.types.basic.Tuple;
import jol.types.basic.TupleSet;
import jol.types.exception.BadKeyException;
import jol.types.table.BasicTable;
import jol.types.table.EventTable;
import jol.types.table.Key;
import jol.types.table.TableName;
import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
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
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.statistics.JobStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageletStatistics;

public class JOLJobManagerImpl implements IJobManager {
    public static final String JOL_SCOPE = "hyrackscc";

    private static final String SCHEDULER_OLG_FILE = "edu/uci/ics/hyracks/controller/clustercontroller/scheduler.olg";

    private final ClusterControllerService ccs;

    private final Runtime jolRuntime;

    private final JobTable jobTable;

    private final OperatorDescriptorTable odTable;

    private final OperatorLocationTable olTable;

    private final ConnectorDescriptorTable cdTable;

    private final ActivityNodeTable anTable;

    private final ActivityConnectionTable acTable;

    private final ActivityBlockedTable abTable;

    private final JobStartTable jobStartTable;

    private final StartMessageTable startMessageTable;

    private final StageletCompleteTable stageletCompleteTable;

    public JOLJobManagerImpl(final ClusterControllerService ccs, Runtime jolRuntime) throws Exception {
        this.ccs = ccs;
        this.jolRuntime = jolRuntime;
        this.jobTable = new JobTable(jolRuntime);
        this.odTable = new OperatorDescriptorTable(jolRuntime);
        this.olTable = new OperatorLocationTable(jolRuntime);
        this.cdTable = new ConnectorDescriptorTable(jolRuntime);
        this.anTable = new ActivityNodeTable(jolRuntime);
        this.acTable = new ActivityConnectionTable(jolRuntime);
        this.abTable = new ActivityBlockedTable(jolRuntime);
        this.jobStartTable = new JobStartTable();
        this.startMessageTable = new StartMessageTable(jolRuntime);
        this.stageletCompleteTable = new StageletCompleteTable(jolRuntime);

        jolRuntime.catalog().register(jobTable);
        jolRuntime.catalog().register(odTable);
        jolRuntime.catalog().register(olTable);
        jolRuntime.catalog().register(cdTable);
        jolRuntime.catalog().register(anTable);
        jolRuntime.catalog().register(acTable);
        jolRuntime.catalog().register(abTable);
        jolRuntime.catalog().register(jobStartTable);
        jolRuntime.catalog().register(startMessageTable);
        jolRuntime.catalog().register(stageletCompleteTable);

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

            @Override
            public void insertion(TupleSet tuples) {
                try {
                    synchronized (JOLJobManagerImpl.this) {
                        for (Tuple t : tuples) {
                            Object[] data = t.toArray();
                            UUID jobId = (UUID) data[0];
                            UUID stageId = (UUID) data[1];
                            JobPlan plan = (JobPlan) data[2];
                            Set<List> ts = (Set<List>) data[3];
                            ClusterControllerService.Phase1Installer[] p1is = new ClusterControllerService.Phase1Installer[ts
                                .size()];
                            int i = 0;
                            for (List t2 : ts) {
                                Object[] t2Data = t2.toArray();
                                p1is[i++] = new ClusterControllerService.Phase1Installer((String) t2Data[0], jobId,
                                    plan, stageId, (Set<ActivityNodeId>) t2Data[1]);
                            }
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
                                p2is[i] = new ClusterControllerService.Phase2Installer((String) t2Data[0], jobId, plan,
                                    stageId, (Set<ActivityNodeId>) t2Data[1], globalPortMap);
                                p3is[i] = new ClusterControllerService.Phase3Installer((String) t2Data[0], jobId,
                                    stageId);
                                ss[i] = new ClusterControllerService.StageStarter((String) t2Data[0], jobId, stageId);
                                ++i;
                            }
                            ccs.runRemote(p2is, null);
                            ccs.runRemote(p3is, null);
                            ccs.runRemote(ss, null);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        jolRuntime.install(JOL_SCOPE, ClassLoader.getSystemResource(SCHEDULER_OLG_FILE));
        jolRuntime.evaluate();
    }

    @Override
    public void advanceJob(JobControl jobControlImpl) throws Exception {

    }

    @Override
    public synchronized UUID createJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
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
        for (Map.Entry<OperatorDescriptorId, IOperatorDescriptor> e : jobSpec.getOperatorMap().entrySet()) {
            IOperatorDescriptor od = e.getValue();
            odTuples.add(OperatorDescriptorTable.createTuple(jobId, od));
            PartitionConstraint pc = od.getPartitionConstraint();
            LocationConstraint[] locationConstraints = pc.getLocationConstraints();
            String[] partitions = new String[locationConstraints.length];
            for (int i = 0; i < locationConstraints.length; ++i) {
                String nodeId = ((AbsoluteLocationConstraint) locationConstraints[i]).getLocationId();
                olTuples.add(OperatorLocationTable.createTuple(jobId, od.getOperatorId(), nodeId));
                partitions[i] = nodeId;
            }
            od.setPartitions(partitions);
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
        jolRuntime.schedule(JOL_SCOPE, ConnectorDescriptorTable.TABLE_NAME, cdTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ActivityNodeTable.TABLE_NAME, anTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ActivityConnectionTable.TABLE_NAME, acTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ActivityBlockedTable.TABLE_NAME, abTuples, null);

        jolRuntime.evaluate();

        return jobId;
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
    public void notifyNodeFailure(String nodeId) {

    }

    @Override
    public synchronized void notifyStageletComplete(UUID jobId, UUID stageId, String nodeId,
        StageletStatistics statistics) throws Exception {
        BasicTupleSet scTuples = new BasicTupleSet();
        scTuples.add(StageletCompleteTable.createTuple(jobId, stageId, nodeId, statistics));

        jolRuntime.schedule(JOL_SCOPE, StageletCompleteTable.TABLE_NAME, scTuples, null);

        jolRuntime.evaluate();
    }

    @Override
    public synchronized void start(UUID jobId) throws Exception {
        BasicTupleSet jsTuples = new BasicTupleSet();
        jsTuples.add(JobStartTable.createTuple(jobId, System.currentTimeMillis()));

        jolRuntime.schedule(JOL_SCOPE, JobStartTable.TABLE_NAME, jsTuples, null);

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

        private static final Class[] SCHEMA = new Class[] {
            UUID.class, JobStatus.class, JobSpecification.class, JobPlan.class, Set.class
        };

        public JobTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createInitialJobTuple(UUID jobId, JobSpecification jobSpec, JobPlan plan) {
            return new Tuple(jobId, JobStatus.INITIALIZED, jobSpec, plan, new HashSet());
        }

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

        private static final Class[] SCHEMA = new Class[] {
            UUID.class, OperatorDescriptorId.class, IOperatorDescriptor.class
        };

        public OperatorDescriptorTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, IOperatorDescriptor od) {
            return new Tuple(jobId, od.getOperatorId(), od);
        }
    }

    /*
     * declare(operatordescriptor, keys(0, 1), {JobId, ODId, OperatorDescriptor})
     */
    private static class OperatorLocationTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "operatorlocation");

        private static Key PRIMARY_KEY = new Key();

        private static final Class[] SCHEMA = new Class[] {
            UUID.class, OperatorDescriptorId.class, String.class
        };

        public OperatorLocationTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, OperatorDescriptorId opId, String nodeId) {
            return new Tuple(jobId, opId, nodeId);
        }
    }

    /*
     * declare(connectordescriptor, keys(0, 1), {JobId, CDId, SrcODId, SrcPort, DestODId, DestPort, ConnectorDescriptor})
     */
    private static class ConnectorDescriptorTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "connectordescriptor");

        private static Key PRIMARY_KEY = new Key(0, 1);

        private static final Class[] SCHEMA = new Class[] {
            UUID.class,
            ConnectorDescriptorId.class,
            OperatorDescriptorId.class,
            Integer.class,
            OperatorDescriptorId.class,
            Integer.class,
            IConnectorDescriptor.class
        };

        public ConnectorDescriptorTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, JobSpecification jobSpec, IConnectorDescriptor conn) {
            IOperatorDescriptor srcOD = jobSpec.getProducer(conn);
            int srcPort = jobSpec.getProducerOutputIndex(conn);
            IOperatorDescriptor destOD = jobSpec.getConsumer(conn);
            int destPort = jobSpec.getConsumerInputIndex(conn);
            Tuple cdTuple = new Tuple(jobId, conn.getConnectorId(), srcOD.getOperatorId(), srcPort, destOD
                .getOperatorId(), destPort, conn);
            return cdTuple;
        }
    }

    /*
     * declare(activitynode, keys(0, 1, 2), {JobId, OperatorId, ActivityId, ActivityNode})
     */
    private static class ActivityNodeTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "activitynode");

        private static Key PRIMARY_KEY = new Key(0, 1, 2);

        private static final Class[] SCHEMA = new Class[] {
            UUID.class, OperatorDescriptorId.class, ActivityNodeId.class, IActivityNode.class
        };

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

        private static final Class[] SCHEMA = new Class[] {
            UUID.class, OperatorDescriptorId.class, Integer.class, Direction.class, ActivityNodeId.class, Integer.class
        };

        public ActivityConnectionTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, IActivityNode aNode, Direction direction, int odPort, int activityPort) {
            return new Tuple(jobId, aNode.getActivityNodeId().getOperatorDescriptorId(), odPort, direction, aNode
                .getActivityNodeId(), activityPort);
        }
    }

    /*
     * declare(activityblocked, keys(0, 1, 2, 3), {JobId, OperatorId, BlockerActivityId, BlockedActivityId})
     */
    private static class ActivityBlockedTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "activityblocked");

        private static Key PRIMARY_KEY = new Key(0, 1, 2, 3, 4);

        private static final Class[] SCHEMA = new Class[] {
            UUID.class,
            OperatorDescriptorId.class,
            ActivityNodeId.class,
            OperatorDescriptorId.class,
            ActivityNodeId.class
        };

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

        private static final Class[] SCHEMA = new Class[] {
            UUID.class, Long.class
        };

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

        private static Key PRIMARY_KEY = new Key(0, 1);

        private static final Class[] SCHEMA = new Class[] {
            UUID.class, UUID.class, JobPlan.class, Set.class
        };

        public StartMessageTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }
    }

    /*
     * declare(stageletcomplete, keys(0, 1, 2), {JobId, StageId, NodeId, StageletStatistics})
     */
    private static class StageletCompleteTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "stageletcomplete");

        private static Key PRIMARY_KEY = new Key(0, 1, 2);

        private static final Class[] SCHEMA = new Class[] {
            UUID.class, UUID.class, String.class, StageletStatistics.class
        };

        public StageletCompleteTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        public static Tuple createTuple(UUID jobId, UUID stageId, String nodeId, StageletStatistics statistics) {
            return new Tuple(jobId, stageId, nodeId, statistics);
        }
    }
}