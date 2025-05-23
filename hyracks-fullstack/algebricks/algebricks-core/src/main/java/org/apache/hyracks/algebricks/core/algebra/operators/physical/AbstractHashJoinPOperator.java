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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

public abstract class AbstractHashJoinPOperator extends AbstractJoinPOperator {

    protected List<LogicalVariable> keysLeftBranch;
    protected List<LogicalVariable> keysRightBranch;

    public AbstractHashJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities) {
        super(kind, partitioningType);
        this.keysLeftBranch = sideLeftOfEqualities;
        this.keysRightBranch = sideRightOfEqualities;
    }

    public List<LogicalVariable> getKeysLeftBranch() {
        return keysLeftBranch;
    }

    public List<LogicalVariable> getKeysRightBranch() {
        return keysRightBranch;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator iop, IOptimizationContext context)
            throws AlgebricksException {
        IPartitioningProperty pp;
        AbstractLogicalOperator op = (AbstractLogicalOperator) iop;

        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            AbstractLogicalOperator op0 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            IPhysicalPropertiesVector pv0 = op0.getPhysicalOperator().getDeliveredProperties();
            AbstractLogicalOperator op1 = (AbstractLogicalOperator) op.getInputs().get(1).getValue();
            IPhysicalPropertiesVector pv1 = op1.getPhysicalOperator().getDeliveredProperties();

            if (pv0 == null || pv1 == null) {
                pp = null;
            } else {
                pp = pv0.getPartitioningProperty();
            }
        } else {
            pp = IPartitioningProperty.UNPARTITIONED;
        }
        this.deliveredProperties = new StructuralPropertiesVector(pp, deliveredLocalProperties(iop, context));
    }

    static void validateNumKeys(List<LogicalVariable> keysLeftBranch, List<LogicalVariable> keysRightBranch) {
        if (keysLeftBranch.size() != keysRightBranch.size()) {
            throw new IllegalStateException("Number of keys of left and right branch are not equal in hash join");
        }
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext ctx) {
        // In a cost-based optimizer, we would also try to propagate the
        // parent's partitioning requirements.
        IPartitioningProperty pp1;
        IPartitioningProperty pp2;
        INodeDomain nodeDomain = ctx.getComputationNodeDomain();
        switch (partitioningType) {
            case PAIRWISE:
                int[][] partitionsMap = ctx.getMetadataProvider().getPartitionsMap(nodeDomain);
                pp1 = OperatorPropertiesUtil.createUnorderedProperty(nodeDomain, partitionsMap, keysLeftBranch);
                pp2 = OperatorPropertiesUtil.createUnorderedProperty(nodeDomain, partitionsMap, keysRightBranch);
                break;
            case BROADCAST:
                pp1 = new RandomPartitioningProperty(nodeDomain);
                pp2 = new BroadcastPartitioningProperty(nodeDomain);
                break;
            default:
                throw new IllegalStateException();
        }

        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[2];
        pv[0] = OperatorPropertiesUtil.checkUnpartitionedAndGetPropertiesVector(op,
                new StructuralPropertiesVector(pp1, null));
        pv[1] = OperatorPropertiesUtil.checkUnpartitionedAndGetPropertiesVector(op,
                new StructuralPropertiesVector(pp2, null));

        IPartitioningRequirementsCoordinator prc;
        switch (kind) {
            case INNER: {
                prc = IPartitioningRequirementsCoordinator.EQCLASS_PARTITIONING_COORDINATOR;
                break;
            }
            case LEFT_OUTER: {
                prc = new IPartitioningRequirementsCoordinator() {

                    @Override
                    public Pair<Boolean, IPartitioningProperty> coordinateRequirements(
                            IPartitioningProperty requirements, IPartitioningProperty firstDeliveredPartitioning,
                            ILogicalOperator op, IOptimizationContext context) throws AlgebricksException {
                        if (firstDeliveredPartitioning != null && requirements != null && firstDeliveredPartitioning
                                .getPartitioningType() == requirements.getPartitioningType()) {
                            switch (requirements.getPartitioningType()) {
                                case UNORDERED_PARTITIONED: {
                                    UnorderedPartitionedProperty unorderedFirstDelivered =
                                            (UnorderedPartitionedProperty) firstDeliveredPartitioning;
                                    Set<LogicalVariable> firstDeliveredVars = unorderedFirstDelivered.getColumnSet();
                                    UnorderedPartitionedProperty unorderedRequired =
                                            (UnorderedPartitionedProperty) requirements;
                                    Set<LogicalVariable> modifiedRequiredVars = new ListSet<>();
                                    Map<LogicalVariable, EquivalenceClass> eqmap = context.getEquivalenceClassMap(op);
                                    Set<LogicalVariable> coveredVars = new ListSet<>();
                                    List<LogicalVariable> keysFirst = (keysRightBranch.containsAll(firstDeliveredVars))
                                            ? keysRightBranch : keysLeftBranch;
                                    List<LogicalVariable> keysSecond =
                                            keysFirst == keysRightBranch ? keysLeftBranch : keysRightBranch;
                                    for (LogicalVariable r : firstDeliveredVars) {
                                        EquivalenceClass ecFirst = eqmap.get(r);
                                        boolean found = false;
                                        int j = 0;
                                        for (LogicalVariable rvar : keysFirst) {
                                            if (!modifiedRequiredVars.contains(keysSecond.get(j))
                                                    && (rvar == r || (ecFirst != null && ecFirst == eqmap.get(rvar)))) {
                                                found = true;
                                                break;
                                            }
                                            j++;
                                        }
                                        if (!found) {
                                            throw new IllegalStateException("Did not find a variable equivalent to " + r
                                                    + " among " + keysFirst);
                                        }
                                        LogicalVariable v2 = keysSecond.get(j);
                                        coveredVars.add(r);
                                        modifiedRequiredVars.add(v2);
                                        if (coveredVars.equals(firstDeliveredVars)) {
                                            break;
                                        }
                                    }
                                    if (!coveredVars.equals(firstDeliveredVars)) {
                                        throw new AlgebricksException(
                                                "Could not modify the original required partitioning " + requirements
                                                        + " to agree with the partitioning property "
                                                        + firstDeliveredPartitioning
                                                        + " delivered by previous input operator. Covered vars: "
                                                        + coveredVars + ". Modified required vars: "
                                                        + modifiedRequiredVars);
                                    }
                                    UnorderedPartitionedProperty unorderedFinalRequired;
                                    if (unorderedRequired.usesPartitionsMap()) {
                                        unorderedFinalRequired = UnorderedPartitionedProperty.ofPartitionsMap(
                                                modifiedRequiredVars, unorderedRequired.getNodeDomain(),
                                                unorderedRequired.getPartitionsMap());
                                    } else {
                                        unorderedFinalRequired = UnorderedPartitionedProperty.of(modifiedRequiredVars,
                                                unorderedRequired.getNodeDomain());
                                    }
                                    return new Pair<>(false, unorderedFinalRequired);
                                }
                                case ORDERED_PARTITIONED: {
                                    throw new NotImplementedException();
                                }
                            }
                        }
                        return new Pair<>(true, requirements);
                    }
                };
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
        return new PhysicalRequirements(pv, prc);
    }

    protected abstract List<ILocalStructuralProperty> deliveredLocalProperties(ILogicalOperator op,
            IOptimizationContext context) throws AlgebricksException;
}
