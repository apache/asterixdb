/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.algebricks.core.jobgen.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint.PartitionConstraintType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;

public class JobBuilder implements IHyracksJobBuilder {

    private final JobSpecification jobSpec;
    private final AlgebricksAbsolutePartitionConstraint clusterLocations;
    private final AlgebricksAbsolutePartitionConstraint countOneLocation;

    private final Map<ILogicalOperator, ArrayList<ILogicalOperator>> outEdges = new HashMap<ILogicalOperator, ArrayList<ILogicalOperator>>();
    private final Map<ILogicalOperator, ArrayList<ILogicalOperator>> inEdges = new HashMap<ILogicalOperator, ArrayList<ILogicalOperator>>();
    private final Map<ILogicalOperator, Pair<IConnectorDescriptor, TargetConstraint>> connectors = new HashMap<ILogicalOperator, Pair<IConnectorDescriptor, TargetConstraint>>();

    private final Map<ILogicalOperator, Pair<IPushRuntimeFactory, RecordDescriptor>> microOps = new HashMap<ILogicalOperator, Pair<IPushRuntimeFactory, RecordDescriptor>>();
    private final Map<IPushRuntimeFactory, ILogicalOperator> revMicroOpMap = new HashMap<IPushRuntimeFactory, ILogicalOperator>();
    private final Map<ILogicalOperator, IOperatorDescriptor> hyracksOps = new HashMap<ILogicalOperator, IOperatorDescriptor>();
    private final Map<ILogicalOperator, AlgebricksPartitionConstraint> pcForMicroOps = new HashMap<ILogicalOperator, AlgebricksPartitionConstraint>();

    private final Map<ILogicalOperator, Integer> algebraicOpBelongingToMetaAsterixOp = new HashMap<ILogicalOperator, Integer>();
    private final Map<Integer, List<Pair<IPushRuntimeFactory, RecordDescriptor>>> metaAsterixOpSkeletons = new HashMap<Integer, List<Pair<IPushRuntimeFactory, RecordDescriptor>>>();
    private final Map<Integer, AlgebricksMetaOperatorDescriptor> metaAsterixOps = new HashMap<Integer, AlgebricksMetaOperatorDescriptor>();
    private final Map<IOperatorDescriptor, AlgebricksPartitionConstraint> partitionConstraintMap = new HashMap<IOperatorDescriptor, AlgebricksPartitionConstraint>();

    private int aodCounter = 0;

    public JobBuilder(JobSpecification jobSpec, AlgebricksAbsolutePartitionConstraint clusterLocations) {
        this.jobSpec = jobSpec;
        this.clusterLocations = clusterLocations;

        // Uses a partition (fixed within a query) for the count constraint count=1.
        // In this way, the SuperActivityRewriter can be faithful to the original JobSpecification.
        // Otherwise, the following query plan:
        // Nested-Loop-Join (count=1)
        //   -- OneToOne Exchange
        //    -- Aggregate (count=1)
        //      ....
        //   -- OneToOne Exchange
        //    -- Aggregate (count=1)
        //      ....
        // might not be able to execute correctly, i.e.,
        // the join-build activity and the join-probe activity get assigned to
        // different partitions.
        int nPartitions = clusterLocations.getLocations().length;
        countOneLocation = new AlgebricksAbsolutePartitionConstraint(
                new String[] { clusterLocations.getLocations()[Math.abs(jobSpec.hashCode() % nPartitions)] });
    }

    @Override
    public void contributeMicroOperator(ILogicalOperator op, IPushRuntimeFactory runtime, RecordDescriptor recDesc) {
        contributeMicroOperator(op, runtime, recDesc, null);
    }

    @Override
    public void contributeMicroOperator(ILogicalOperator op, IPushRuntimeFactory runtime, RecordDescriptor recDesc,
            AlgebricksPartitionConstraint pc) {
        microOps.put(op, new Pair<IPushRuntimeFactory, RecordDescriptor>(runtime, recDesc));
        revMicroOpMap.put(runtime, op);
        if (pc != null) {
            pcForMicroOps.put(op, pc);
        }
        AbstractLogicalOperator logicalOp = (AbstractLogicalOperator) op;
        if (logicalOp.getExecutionMode() == ExecutionMode.UNPARTITIONED && pc == null) {
            AlgebricksPartitionConstraint apc = countOneLocation;
            pcForMicroOps.put(logicalOp, apc);
        }
    }

    @Override
    public void contributeConnector(ILogicalOperator exchgOp, IConnectorDescriptor conn) {
        connectors.put(exchgOp, new Pair<IConnectorDescriptor, TargetConstraint>(conn, null));
    }

    @Override
    public void contributeConnectorWithTargetConstraint(ILogicalOperator exchgOp, IConnectorDescriptor conn,
            TargetConstraint numberOfTargetPartitions) {
        connectors.put(exchgOp, new Pair<IConnectorDescriptor, TargetConstraint>(conn, numberOfTargetPartitions));
    }

    @Override
    public void contributeGraphEdge(ILogicalOperator src, int srcOutputIndex, ILogicalOperator dest,
            int destInputIndex) {
        ArrayList<ILogicalOperator> outputs = outEdges.get(src);
        if (outputs == null) {
            outputs = new ArrayList<ILogicalOperator>();
            outEdges.put(src, outputs);
        }
        addAtPos(outputs, dest, srcOutputIndex);

        ArrayList<ILogicalOperator> inp = inEdges.get(dest);
        if (inp == null) {
            inp = new ArrayList<ILogicalOperator>();
            inEdges.put(dest, inp);
        }
        addAtPos(inp, src, destInputIndex);
    }

    @Override
    public void contributeHyracksOperator(ILogicalOperator op, IOperatorDescriptor opDesc) {
        hyracksOps.put(op, opDesc);
    }

    @Override
    public void contributeAlgebricksPartitionConstraint(IOperatorDescriptor opDesc, AlgebricksPartitionConstraint apc) {
        if (apc.getPartitionConstraintType() == PartitionConstraintType.COUNT) {
            AlgebricksCountPartitionConstraint constraint = (AlgebricksCountPartitionConstraint) apc;
            if (constraint.getCount() == 1) {
                apc = countOneLocation;
            }
        }
        partitionConstraintMap.put(opDesc, apc);
    }

    @Override
    public JobSpecification getJobSpec() {
        return jobSpec;
    }

    @Override
    public void buildSpec(List<ILogicalOperator> roots) throws AlgebricksException {
        buildAsterixComponents();
        Map<IConnectorDescriptor, TargetConstraint> tgtConstraints = setupConnectors();
        for (ILogicalOperator r : roots) {
            IOperatorDescriptor opDesc = findOpDescForAlgebraicOp(r);
            jobSpec.addRoot(opDesc);
        }
        setAllPartitionConstraints(tgtConstraints);
    }

    private void setAllPartitionConstraints(Map<IConnectorDescriptor, TargetConstraint> tgtConstraints) {
        List<OperatorDescriptorId> roots = jobSpec.getRoots();
        setSpecifiedPartitionConstraints();
        for (OperatorDescriptorId rootId : roots) {
            setPartitionConstraintsBottomup(rootId, tgtConstraints, null, false);
        }
        for (OperatorDescriptorId rootId : roots) {
            setPartitionConstraintsTopdown(rootId, tgtConstraints, null);
        }
        for (OperatorDescriptorId rootId : roots) {
            setPartitionConstraintsBottomup(rootId, tgtConstraints, null, true);
        }
    }

    private void setSpecifiedPartitionConstraints() {
        for (ILogicalOperator op : pcForMicroOps.keySet()) {
            AlgebricksPartitionConstraint pc = pcForMicroOps.get(op);
            Integer k = algebraicOpBelongingToMetaAsterixOp.get(op);
            AlgebricksMetaOperatorDescriptor amod = metaAsterixOps.get(k);
            partitionConstraintMap.put(amod, pc);
        }
        for (IOperatorDescriptor opDesc : partitionConstraintMap.keySet()) {
            AlgebricksPartitionConstraint pc = partitionConstraintMap.get(opDesc);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, opDesc, pc);
        }
    }

    private void setPartitionConstraintsTopdown(OperatorDescriptorId opId,
            Map<IConnectorDescriptor, TargetConstraint> tgtConstraints, IOperatorDescriptor parentOp) {
        List<IConnectorDescriptor> opInputs = jobSpec.getOperatorInputMap().get(opId);
        AlgebricksPartitionConstraint opConstraint = null;
        IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(opId);
        if (opInputs != null) {
            for (IConnectorDescriptor conn : opInputs) {
                ConnectorDescriptorId cid = conn.getConnectorId();
                org.apache.commons.lang3.tuple.Pair<org.apache.commons.lang3.tuple.Pair<IOperatorDescriptor, Integer>, org.apache.commons.lang3.tuple.Pair<IOperatorDescriptor, Integer>> p = jobSpec
                        .getConnectorOperatorMap().get(cid);
                IOperatorDescriptor src = p.getLeft().getLeft();
                TargetConstraint constraint = tgtConstraints.get(conn);
                if (constraint != null) {
                    if (constraint == TargetConstraint.SAME_COUNT) {
                        opConstraint = partitionConstraintMap.get(opDesc);
                        if (partitionConstraintMap.get(src) == null) {
                            if (opConstraint != null) {
                                partitionConstraintMap.put(src, opConstraint);
                                AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, src,
                                        opConstraint);
                            }
                        }
                    }
                }
                // Post Order DFS
                setPartitionConstraintsTopdown(src.getOperatorId(), tgtConstraints, opDesc);
            }
        }
    }

    private void setPartitionConstraintsBottomup(OperatorDescriptorId opId,
            Map<IConnectorDescriptor, TargetConstraint> tgtConstraints, IOperatorDescriptor parentOp,
            boolean finalPass) {
        List<IConnectorDescriptor> opInputs = jobSpec.getOperatorInputMap().get(opId);
        AlgebricksPartitionConstraint opConstraint = null;
        IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(opId);
        if (opInputs != null) {
            for (IConnectorDescriptor conn : opInputs) {
                ConnectorDescriptorId cid = conn.getConnectorId();
                org.apache.commons.lang3.tuple.Pair<org.apache.commons.lang3.tuple.Pair<IOperatorDescriptor, Integer>, org.apache.commons.lang3.tuple.Pair<IOperatorDescriptor, Integer>> p = jobSpec
                        .getConnectorOperatorMap().get(cid);
                IOperatorDescriptor src = p.getLeft().getLeft();
                // Pre-order DFS
                setPartitionConstraintsBottomup(src.getOperatorId(), tgtConstraints, opDesc, finalPass);
                TargetConstraint constraint = tgtConstraints.get(conn);
                if (constraint != null) {
                    switch (constraint) {
                        case ONE: {
                            opConstraint = countOneLocation;
                            break;
                        }
                        case SAME_COUNT: {
                            opConstraint = partitionConstraintMap.get(src);
                            break;
                        }
                    }
                }
            }
        }
        if (partitionConstraintMap.get(opDesc) == null) {
            if (finalPass && opConstraint == null && (opInputs == null || opInputs.size() == 0)) {
                opConstraint = countOneLocation;
            }
            if (finalPass && opConstraint == null) {
                opConstraint = clusterLocations;
            }
            // Sets up the location constraint.
            if (opConstraint != null) {
                partitionConstraintMap.put(opDesc, opConstraint);
                AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, opDesc, opConstraint);
            }
        }
    }

    private Map<IConnectorDescriptor, TargetConstraint> setupConnectors() throws AlgebricksException {
        Map<IConnectorDescriptor, TargetConstraint> tgtConstraints = new HashMap<IConnectorDescriptor, TargetConstraint>();
        for (ILogicalOperator exchg : connectors.keySet()) {
            ILogicalOperator inOp = inEdges.get(exchg).get(0);
            ILogicalOperator outOp = outEdges.get(exchg).get(0);
            IOperatorDescriptor inOpDesc = findOpDescForAlgebraicOp(inOp);
            IOperatorDescriptor outOpDesc = findOpDescForAlgebraicOp(outOp);
            Pair<IConnectorDescriptor, TargetConstraint> connPair = connectors.get(exchg);
            IConnectorDescriptor conn = connPair.first;
            int producerPort = outEdges.get(inOp).indexOf(exchg);
            int consumerPort = inEdges.get(outOp).indexOf(exchg);
            jobSpec.connect(conn, inOpDesc, producerPort, outOpDesc, consumerPort);
            if (connPair.second != null) {
                tgtConstraints.put(conn, connPair.second);
            }
        }
        return tgtConstraints;
    }

    private IOperatorDescriptor findOpDescForAlgebraicOp(ILogicalOperator op) throws AlgebricksException {
        IOperatorDescriptor hOpDesc = hyracksOps.get(op);
        if (hOpDesc != null) {
            return hOpDesc;
        }
        Integer metaOpKey = algebraicOpBelongingToMetaAsterixOp.get(op);
        if (metaOpKey == null) {
            throw new AlgebricksException("Could not generate operator descriptor for operator " + op);
        }
        return metaAsterixOps.get(metaOpKey);
    }

    private void buildAsterixComponents() {
        for (ILogicalOperator aop : microOps.keySet()) {
            addMicroOpToMetaRuntimeOp(aop);
        }
        for (Integer k : metaAsterixOpSkeletons.keySet()) {
            List<Pair<IPushRuntimeFactory, RecordDescriptor>> opContents = metaAsterixOpSkeletons.get(k);
            AlgebricksMetaOperatorDescriptor amod = buildMetaAsterixOpDesc(opContents);
            metaAsterixOps.put(k, amod);
        }
    }

    private AlgebricksMetaOperatorDescriptor buildMetaAsterixOpDesc(
            List<Pair<IPushRuntimeFactory, RecordDescriptor>> opContents) {
        // RecordDescriptor outputRecordDesc = null;
        int n = opContents.size();
        IPushRuntimeFactory[] runtimeFactories = new IPushRuntimeFactory[n];
        RecordDescriptor[] internalRecordDescriptors = new RecordDescriptor[n];
        int i = 0;
        for (Pair<IPushRuntimeFactory, RecordDescriptor> p : opContents) {
            runtimeFactories[i] = p.first;
            internalRecordDescriptors[i] = p.second;
            // if (i == n - 1) {
            // outputRecordDesc = p.second;
            // }
            i++;
        }
        ILogicalOperator lastLogicalOp = revMicroOpMap.get(runtimeFactories[n - 1]);
        ArrayList<ILogicalOperator> outOps = outEdges.get(lastLogicalOp);
        int outArity = (outOps == null) ? 0 : outOps.size();
        ILogicalOperator firstLogicalOp = revMicroOpMap.get(runtimeFactories[0]);
        ArrayList<ILogicalOperator> inOps = inEdges.get(firstLogicalOp);
        int inArity = (inOps == null) ? 0 : inOps.size();
        // boolean isLeafOp = inEdges.get(firstLogicalOp) == null;
        return new AlgebricksMetaOperatorDescriptor(jobSpec, inArity, outArity, runtimeFactories,
                internalRecordDescriptors);
    }

    private void addMicroOpToMetaRuntimeOp(ILogicalOperator aop) {
        Integer k = algebraicOpBelongingToMetaAsterixOp.get(aop);
        if (k == null) {
            k = createNewMetaOpInfo(aop);
        }
        ArrayList<ILogicalOperator> destList = outEdges.get(aop);
        if (destList == null || destList.size() != 1) {
            // for now, we only support linear plans inside meta-ops.
            return;
        }
        ILogicalOperator dest = destList.get(0);
        Integer j = algebraicOpBelongingToMetaAsterixOp.get(dest);
        if (j == null && microOps.get(dest) != null) {
            algebraicOpBelongingToMetaAsterixOp.put(dest, k);
            List<Pair<IPushRuntimeFactory, RecordDescriptor>> aodContent1 = metaAsterixOpSkeletons.get(k);
            aodContent1.add(microOps.get(dest));
        } else if (j != null && j.intValue() != k.intValue()) {
            // merge the j component into the k component
            List<Pair<IPushRuntimeFactory, RecordDescriptor>> aodContent1 = metaAsterixOpSkeletons.get(k);
            List<Pair<IPushRuntimeFactory, RecordDescriptor>> aodContent2 = metaAsterixOpSkeletons.get(j);
            aodContent1.addAll(aodContent2);
            metaAsterixOpSkeletons.remove(j);
            for (ILogicalOperator m : algebraicOpBelongingToMetaAsterixOp.keySet()) {
                Integer g = algebraicOpBelongingToMetaAsterixOp.get(m);
                if (g.intValue() == j.intValue()) {
                    algebraicOpBelongingToMetaAsterixOp.put(m, k);
                }
            }
        }

    }

    private int createNewMetaOpInfo(ILogicalOperator aop) {
        int n = aodCounter;
        aodCounter++;
        List<Pair<IPushRuntimeFactory, RecordDescriptor>> metaOpContents = new ArrayList<Pair<IPushRuntimeFactory, RecordDescriptor>>();
        metaOpContents.add(microOps.get(aop));
        metaAsterixOpSkeletons.put(n, metaOpContents);
        algebraicOpBelongingToMetaAsterixOp.put(aop, n);
        return n;
    }

    private <E> void addAtPos(ArrayList<E> a, E elem, int pos) {
        int n = a.size();
        if (n > pos) {
            a.set(pos, elem);
        } else {
            for (int k = n; k < pos; k++) {
                a.add(null);
            }
            a.add(elem);
        }
    }

}
