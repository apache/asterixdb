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
package edu.uci.ics.hyracks.control.cc.scheduler;

import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.IConstraintAcceptor;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.ActivityCluster;
import edu.uci.ics.hyracks.control.cc.job.IConnectorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.IOperatorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.PlanUtils;
import edu.uci.ics.hyracks.control.cc.job.manager.events.JobCleanupEvent;

public class JobRunStateMachine {
    private final ClusterControllerService ccs;

    private final JobRun jobRun;

    private final Set<ActivityCluster> completedClusters;

    private final Set<ActivityCluster> inProgressClusters;

    private PartitionConstraintSolver solver;

    private ActivityCluster rootActivityCluster;

    public JobRunStateMachine(ClusterControllerService ccs, JobRun jobRun) {
        this.ccs = ccs;
        this.jobRun = jobRun;
        completedClusters = new HashSet<ActivityCluster>();
        inProgressClusters = new HashSet<ActivityCluster>();
    }

    public PartitionConstraintSolver getSolver() {
        return solver;
    }

    private void findRunnableActivityClusters(Set<ActivityCluster> frontier, ActivityCluster candidate) {
        if (completedClusters.contains(candidate) || frontier.contains(candidate)
                || inProgressClusters.contains(candidate)) {
            return;
        }
        boolean runnable = true;
        for (ActivityCluster s : candidate.getDependencies()) {
            if (!completedClusters.contains(s)) {
                runnable = false;
                findRunnableActivityClusters(frontier, s);
            }
        }
        if (runnable && candidate != rootActivityCluster) {
            frontier.add(candidate);
        }
    }

    private void findRunnableActivityClusters(Set<ActivityCluster> frontier) {
        findRunnableActivityClusters(frontier, rootActivityCluster);
    }

    public void schedule() throws HyracksException {
        try {
            solver = new PartitionConstraintSolver();
            final JobActivityGraph jag = jobRun.getJobActivityGraph();
            final ICCApplicationContext appCtx = ccs.getApplicationMap().get(jag.getApplicationName());
            JobSpecification spec = jag.getJobSpecification();
            final Set<Constraint> contributedConstraints = new HashSet<Constraint>();
            final IConstraintAcceptor acceptor = new IConstraintAcceptor() {
                @Override
                public void addConstraint(Constraint constraint) {
                    contributedConstraints.add(constraint);
                }
            };
            PlanUtils.visit(spec, new IOperatorDescriptorVisitor() {
                @Override
                public void visit(IOperatorDescriptor op) {
                    op.contributeSchedulingConstraints(acceptor, jag, appCtx);
                }
            });
            PlanUtils.visit(spec, new IConnectorDescriptorVisitor() {
                @Override
                public void visit(IConnectorDescriptor conn) {
                    conn.contributeSchedulingConstraints(acceptor, jag, appCtx);
                }
            });
            contributedConstraints.addAll(spec.getUserConstraints());
            solver.addConstraints(contributedConstraints);

            ActivityClusterGraphBuilder acgb = new ActivityClusterGraphBuilder(jobRun);
            rootActivityCluster = acgb.inferStages(jag);
            startRunnableActivityClusters();
        } catch (Exception e) {
            e.printStackTrace();
            ccs.getJobQueue().schedule(new JobCleanupEvent(ccs, jobRun.getJobId(), JobStatus.FAILURE, e));
            throw new HyracksException(e);
        }
    }

    private void startRunnableActivityClusters() throws HyracksException {
        Set<ActivityCluster> runnableClusters = new HashSet<ActivityCluster>();
        findRunnableActivityClusters(runnableClusters);
        if (runnableClusters.isEmpty() && inProgressClusters.isEmpty()) {
            ccs.getJobQueue().schedule(new JobCleanupEvent(ccs, jobRun.getJobId(), JobStatus.TERMINATED, null));
            return;
        }
        for (ActivityCluster ac : runnableClusters) {
            inProgressClusters.add(ac);
            TaskClusterBuilder tcb = new TaskClusterBuilder(jobRun, solver);
            tcb.buildTaskClusters(ac);
            ActivityClusterStateMachine acsm = new ActivityClusterStateMachine(ccs, this, ac);
            ac.setStateMachine(acsm);
            acsm.schedule();
        }
    }

    public void notifyActivityClusterFailure(ActivityCluster ac, Exception exception) throws HyracksException {
        for (ActivityCluster ac2 : inProgressClusters) {
            abortActivityCluster(ac2);
        }
        jobRun.setStatus(JobStatus.FAILURE, exception);
    }

    private void abortActivityCluster(ActivityCluster ac) throws HyracksException {
        ac.getStateMachine().abort();
    }

    public void notifyActivityClusterComplete(ActivityCluster ac) throws HyracksException {
        completedClusters.add(ac);
        inProgressClusters.remove(ac);
        startRunnableActivityClusters();
    }

    public void notifyNodeFailures(Set<String> deadNodes) throws HyracksException {
        jobRun.getPartitionMatchMaker().notifyNodeFailures(deadNodes);
        for (ActivityCluster ac : completedClusters) {
            ac.getStateMachine().notifyNodeFailures(deadNodes);
        }
        for (ActivityCluster ac : inProgressClusters) {
            ac.getStateMachine().notifyNodeFailures(deadNodes);
        }
        for (ActivityCluster ac : inProgressClusters) {
            ActivityClusterStateMachine acsm = ac.getStateMachine();
            if (acsm.canMakeProgress()) {
                acsm.schedule();
            }
        }
    }
}