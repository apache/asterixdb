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
import java.util.Map;
import java.util.UUID;

import jol.core.Runtime;
import jol.types.basic.BasicTupleSet;
import jol.types.basic.Tuple;
import jol.types.table.BasicTable;
import jol.types.table.Key;
import jol.types.table.TableName;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.Direction;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.statistics.JobStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageletStatistics;

public class JOLJobManagerImpl implements IJobManager {
    public static final String JOL_SCOPE = "hyrackscc";

    private static final String SCHEDULER_OLG_FILE = "edu/uci/ics/hyracks/controller/clustercontroller/scheduler.olg";

    private final Runtime jolRuntime;

    private final JobTable jobTable;

    private final OperatorDescriptorTable odTable;

    private final ConnectorDescriptorTable cdTable;

    private final ActivityNodeTable anTable;

    private final ActivityConnectionTable acTable;

    private final ActivityBlockedTable abTable;

    public JOLJobManagerImpl(Runtime jolRuntime) throws Exception {
        this.jolRuntime = jolRuntime;
        this.jobTable = new JobTable(jolRuntime);
        this.odTable = new OperatorDescriptorTable(jolRuntime);
        this.cdTable = new ConnectorDescriptorTable(jolRuntime);
        this.anTable = new ActivityNodeTable(jolRuntime);
        this.acTable = new ActivityConnectionTable(jolRuntime);
        this.abTable = new ActivityBlockedTable(jolRuntime);

        jolRuntime.catalog().register(jobTable);
        jolRuntime.catalog().register(odTable);
        jolRuntime.catalog().register(cdTable);
        jolRuntime.catalog().register(anTable);
        jolRuntime.catalog().register(acTable);
        jolRuntime.catalog().register(abTable);

        jolRuntime.install(JOL_SCOPE, ClassLoader.getSystemResource(SCHEDULER_OLG_FILE));
        jolRuntime.evaluate();
    }

    @Override
    public void advanceJob(JobControl jobControlImpl) throws Exception {

    }

    @Override
    public UUID createJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        final UUID jobId = UUID.randomUUID();

        BasicTupleSet jobTuples = new BasicTupleSet(JobTable.createInitialJobTuple(jobId, jobFlags));

        final BasicTupleSet anTuples = new BasicTupleSet();
        final BasicTupleSet acTuples = new BasicTupleSet();
        final BasicTupleSet abTuples = new BasicTupleSet();
        IActivityGraphBuilder gBuilder = new IActivityGraphBuilder() {
            @Override
            public void addTask(IActivityNode task) {
                anTuples.add(ActivityNodeTable.createTuple(jobId, task));
            }

            @Override
            public void addTargetEdge(int operatorOutputIndex, IActivityNode task, int taskOutputIndex) {
                acTuples.add(ActivityConnectionTable.createTuple(jobId, task, Direction.OUTPUT, operatorOutputIndex,
                    taskOutputIndex));
            }

            @Override
            public void addSourceEdge(int operatorInputIndex, IActivityNode task, int taskInputIndex) {
                acTuples.add(ActivityConnectionTable.createTuple(jobId, task, Direction.INPUT, operatorInputIndex,
                    taskInputIndex));
            }

            @Override
            public void addBlockingEdge(IActivityNode blocker, IActivityNode blocked) {
                abTuples.add(ActivityBlockedTable.createTuple(jobId, blocker, blocked));
            }
        };

        BasicTupleSet odTuples = new BasicTupleSet();
        for (Map.Entry<OperatorDescriptorId, IOperatorDescriptor> e : jobSpec.getOperatorMap().entrySet()) {
            IOperatorDescriptor od = e.getValue();
            odTuples.add(OperatorDescriptorTable.createTuple(jobId, od));
            od.contributeTaskGraph(gBuilder);
        }

        BasicTupleSet cdTuples = new BasicTupleSet();
        for (Map.Entry<ConnectorDescriptorId, IConnectorDescriptor> e : jobSpec.getConnectorMap().entrySet()) {
            cdTuples.add(ConnectorDescriptorTable.createTuple(jobId, jobSpec, e.getValue()));
        }

        jolRuntime.schedule(JOL_SCOPE, JobTable.TABLE_NAME, jobTuples, null);
        jolRuntime.schedule(JOL_SCOPE, OperatorDescriptorTable.TABLE_NAME, odTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ConnectorDescriptorTable.TABLE_NAME, cdTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ActivityNodeTable.TABLE_NAME, anTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ActivityConnectionTable.TABLE_NAME, acTuples, null);
        jolRuntime.schedule(JOL_SCOPE, ActivityBlockedTable.TABLE_NAME, abTuples, null);

        jolRuntime.evaluate();

        return jobId;
    }

    @Override
    public JobStatus getJobStatus(UUID jobId) {
        return null;
    }

    @Override
    public void notifyNodeFailure(String nodeId) {

    }

    @Override
    public void notifyStageletComplete(UUID jobId, UUID stageId, String nodeId, StageletStatistics statistics)
        throws Exception {
    }

    @Override
    public void start(UUID jobId) throws Exception {

    }

    @Override
    public JobStatistics waitForCompletion(UUID jobId) throws Exception {
        return null;
    }

    /*
     * declare(job, keys(0), {JobId, Flags, Status})
     */
    private static class JobTable extends BasicTable {
        private static TableName TABLE_NAME = new TableName(JOL_SCOPE, "job");

        private static Key PRIMARY_KEY = new Key(0);

        private static final Class[] SCHEMA = new Class[] {
            UUID.class, EnumSet.class, JobStatus.class, PerJobCounter.class
        };

        public JobTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createInitialJobTuple(UUID jobId, EnumSet<JobFlag> jobFlags) {
            return new Tuple(jobId, jobFlags, JobStatus.INITIALIZED, new PerJobCounter());
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
     * declare(activitynode, keys(0), {JobId, OperatorId, ActivityId, ActivityNode})
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

        private static Key PRIMARY_KEY = new Key(0);

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

        private static Key PRIMARY_KEY = new Key(0);

        private static final Class[] SCHEMA = new Class[] {
            UUID.class, OperatorDescriptorId.class, ActivityNodeId.class, ActivityNodeId.class
        };

        public ActivityBlockedTable(Runtime context) {
            super(context, TABLE_NAME, PRIMARY_KEY, SCHEMA);
        }

        static Tuple createTuple(UUID jobId, IActivityNode blocker, IActivityNode blocked) {
            OperatorDescriptorId odId = blocker.getActivityNodeId().getOperatorDescriptorId();
            return new Tuple(jobId, odId, blocker.getActivityNodeId(), blocked.getActivityNodeId());
        }
    }
}