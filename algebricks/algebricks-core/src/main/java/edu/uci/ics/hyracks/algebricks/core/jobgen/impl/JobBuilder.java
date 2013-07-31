/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.algebricks.core.jobgen.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class JobBuilder implements IHyracksJobBuilder {

    private JobSpecification jobSpec;
    private AlgebricksPartitionConstraint clusterLocations;

    private Map<ILogicalOperator, ArrayList<ILogicalOperator>> outEdges = new HashMap<ILogicalOperator, ArrayList<ILogicalOperator>>();
    private Map<ILogicalOperator, ArrayList<ILogicalOperator>> inEdges = new HashMap<ILogicalOperator, ArrayList<ILogicalOperator>>();
    private Map<ILogicalOperator, Pair<IConnectorDescriptor, TargetConstraint>> connectors = new HashMap<ILogicalOperator, Pair<IConnectorDescriptor, TargetConstraint>>();

    private Map<ILogicalOperator, Pair<IPushRuntimeFactory, RecordDescriptor>> microOps = new HashMap<ILogicalOperator, Pair<IPushRuntimeFactory, RecordDescriptor>>();
    private Map<IPushRuntimeFactory, ILogicalOperator> revMicroOpMap = new HashMap<IPushRuntimeFactory, ILogicalOperator>();
    private Map<ILogicalOperator, IOperatorDescriptor> hyracksOps = new HashMap<ILogicalOperator, IOperatorDescriptor>();
    private Map<ILogicalOperator, AlgebricksPartitionConstraint> pcForMicroOps = new HashMap<ILogicalOperator, AlgebricksPartitionConstraint>();

    private int aodCounter = 0;
    private Map<ILogicalOperator, Integer> algebraicOpBelongingToMetaAsterixOp = new HashMap<ILogicalOperator, Integer>();
    private Map<Integer, List<Pair<IPushRuntimeFactory, RecordDescriptor>>> metaAsterixOpSkeletons = new HashMap<Integer, List<Pair<IPushRuntimeFactory, RecordDescriptor>>>();
    private Map<Integer, AlgebricksMetaOperatorDescriptor> metaAsterixOps = new HashMap<Integer, AlgebricksMetaOperatorDescriptor>();
    private final Map<IOperatorDescriptor, AlgebricksPartitionConstraint> partitionConstraintMap = new HashMap<IOperatorDescriptor, AlgebricksPartitionConstraint>();

    public JobBuilder(JobSpecification jobSpec, AlgebricksPartitionConstraint clusterLocations) {
        this.jobSpec = jobSpec;
        this.clusterLocations = clusterLocations;
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
            AlgebricksPartitionConstraint apc = new AlgebricksCountPartitionConstraint(1);
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
    public void contributeGraphEdge(ILogicalOperator src, int srcOutputIndex, ILogicalOperator dest, int destInputIndex) {
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
            setPartitionConstraintsDFS(rootId, tgtConstraints, null);
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

    private void setPartitionConstraintsDFS(OperatorDescriptorId opId,
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
                // DFS
                setPartitionConstraintsDFS(src.getOperatorId(), tgtConstraints, opDesc);

                TargetConstraint constraint = tgtConstraints.get(conn);
                if (constraint != null) {
                    switch (constraint) {
                        case ONE: {
                            opConstraint = new AlgebricksCountPartitionConstraint(1);
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
            if (opConstraint == null) {
                if (parentOp != null) {
                    AlgebricksPartitionConstraint pc = partitionConstraintMap.get(parentOp);
                    if (pc != null) {
                        opConstraint = pc;
                    } else if (opInputs == null || opInputs.size() == 0) {
                        opConstraint = new AlgebricksCountPartitionConstraint(1);
                    }
                }
                if (opConstraint == null) {
                    opConstraint = clusterLocations;
                }
            }
            partitionConstraintMap.put(opDesc, opConstraint);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, opDesc, opConstraint);
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
