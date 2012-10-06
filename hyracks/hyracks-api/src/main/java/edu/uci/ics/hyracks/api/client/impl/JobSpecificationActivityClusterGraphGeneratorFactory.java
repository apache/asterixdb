package edu.uci.ics.hyracks.api.client.impl;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.IConstraintAcceptor;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.ActivityClusterGraph;
import edu.uci.ics.hyracks.api.job.IActivityClusterGraphGenerator;
import edu.uci.ics.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class JobSpecificationActivityClusterGraphGeneratorFactory implements IActivityClusterGraphGeneratorFactory {
    private static final long serialVersionUID = 1L;

    private final JobSpecification spec;

    public JobSpecificationActivityClusterGraphGeneratorFactory(JobSpecification jobSpec) {
        this.spec = jobSpec;
    }

    @Override
    public IActivityClusterGraphGenerator createActivityClusterGraphGenerator(String appName, JobId jobId,
            final ICCApplicationContext ccAppCtx, EnumSet<JobFlag> jobFlags) throws HyracksException {
        final JobActivityGraphBuilder builder = new JobActivityGraphBuilder(spec, jobFlags);
        PlanUtils.visit(spec, new IConnectorDescriptorVisitor() {
            @Override
            public void visit(IConnectorDescriptor conn) throws HyracksException {
                builder.addConnector(conn);
            }
        });
        PlanUtils.visit(spec, new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) {
                op.contributeActivities(builder);
            }
        });
        builder.finish();
        final JobActivityGraph jag = builder.getActivityGraph();
        ActivityClusterGraphBuilder acgb = new ActivityClusterGraphBuilder();

        final ActivityClusterGraph acg = acgb.inferActivityClusters(jobId, jag);
        acg.setFrameSize(spec.getFrameSize());
        acg.setMaxReattempts(spec.getMaxReattempts());
        acg.setJobletEventListenerFactory(spec.getJobletEventListenerFactory());
        acg.setGlobalJobDataFactory(spec.getGlobalJobDataFactory());
        final Set<Constraint> constraints = new HashSet<Constraint>();
        final IConstraintAcceptor acceptor = new IConstraintAcceptor() {
            @Override
            public void addConstraint(Constraint constraint) {
                constraints.add(constraint);
            }
        };
        PlanUtils.visit(spec, new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) {
                op.contributeSchedulingConstraints(acceptor, ccAppCtx);
            }
        });
        PlanUtils.visit(spec, new IConnectorDescriptorVisitor() {
            @Override
            public void visit(IConnectorDescriptor conn) {
                conn.contributeSchedulingConstraints(acceptor, acg.getConnectorMap().get(conn.getConnectorId()),
                        ccAppCtx);
            }
        });
        constraints.addAll(spec.getUserConstraints());
        return new IActivityClusterGraphGenerator() {
            @Override
            public ActivityClusterGraph initialize() {
                return acg;
            }

            @Override
            public Set<Constraint> getConstraints() {
                return constraints;
            }
        };
    }
}