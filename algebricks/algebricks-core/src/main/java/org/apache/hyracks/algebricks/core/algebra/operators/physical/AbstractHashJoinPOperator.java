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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.ListSet;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;

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

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator iop,
            IPhysicalPropertiesVector reqdByParent) {
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[2];
        // In a cost-based optimizer, we would also try to propagate the
        // parent's partitioning requirements.
        AbstractLogicalOperator op = (AbstractLogicalOperator) iop;

        IPartitioningProperty pp1 = null;
        IPartitioningProperty pp2 = null;
        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            switch (partitioningType) {
                case PAIRWISE: {
                    pp1 = new UnorderedPartitionedProperty(new ListSet<LogicalVariable>(keysLeftBranch), null);
                    pp2 = new UnorderedPartitionedProperty(new ListSet<LogicalVariable>(keysRightBranch), null);
                    break;
                }
                case BROADCAST: {
                    pp2 = new BroadcastPartitioningProperty(null);
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
        }

        pv[0] = new StructuralPropertiesVector(pp1, null);
        pv[1] = new StructuralPropertiesVector(pp2, null);

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
                        if (firstDeliveredPartitioning != null
                                && firstDeliveredPartitioning.getPartitioningType() == requirements
                                        .getPartitioningType()) {
                            switch (requirements.getPartitioningType()) {
                                case UNORDERED_PARTITIONED: {
                                    UnorderedPartitionedProperty upp1 = (UnorderedPartitionedProperty) firstDeliveredPartitioning;
                                    Set<LogicalVariable> set1 = upp1.getColumnSet();
                                    UnorderedPartitionedProperty uppreq = (UnorderedPartitionedProperty) requirements;
                                    Set<LogicalVariable> modifuppreq = new ListSet<LogicalVariable>();
                                    Map<LogicalVariable, EquivalenceClass> eqmap = context.getEquivalenceClassMap(op);
                                    Set<LogicalVariable> covered = new ListSet<LogicalVariable>();
                                    for (LogicalVariable r : uppreq.getColumnSet()) {
                                        EquivalenceClass ecSnd = eqmap.get(r);
                                        boolean found = false;
                                        int j = 0;
                                        for (LogicalVariable rvar : keysRightBranch) {
                                            if (rvar == r || ecSnd != null && eqmap.get(rvar) == ecSnd) {
                                                found = true;
                                                break;
                                            }
                                            j++;
                                        }
                                        if (!found) {
                                            throw new IllegalStateException("Did not find a variable equivalent to "
                                                    + r + " among " + keysRightBranch);
                                        }
                                        LogicalVariable v2 = keysLeftBranch.get(j);
                                        EquivalenceClass ecFst = eqmap.get(v2);
                                        for (LogicalVariable vset1 : set1) {
                                            if (vset1 == v2 || ecFst != null && eqmap.get(vset1) == ecFst) {
                                                covered.add(vset1);
                                                modifuppreq.add(r);
                                                break;
                                            }
                                        }
                                    }
                                    if (!covered.equals(set1)) {
                                        throw new AlgebricksException("Could not modify " + requirements
                                                + " to agree with partitioning property " + firstDeliveredPartitioning
                                                + " delivered by previous input operator.");
                                    }
                                    UnorderedPartitionedProperty upp2 = new UnorderedPartitionedProperty(modifuppreq,
                                            requirements.getNodeDomain());
                                    return new Pair<Boolean, IPartitioningProperty>(false, upp2);
                                }
                                case ORDERED_PARTITIONED: {
                                    throw new NotImplementedException();
                                }
                            }
                        }
                        return new Pair<Boolean, IPartitioningProperty>(true, requirements);
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
