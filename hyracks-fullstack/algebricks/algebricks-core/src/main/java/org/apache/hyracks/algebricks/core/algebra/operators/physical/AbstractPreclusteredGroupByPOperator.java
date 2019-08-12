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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalGroupingProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.PropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

public abstract class AbstractPreclusteredGroupByPOperator extends AbstractGroupByPOperator {

    protected AbstractPreclusteredGroupByPOperator(List<LogicalVariable> columnList) {
        super(columnList);
    }

    // Obs: We don't propagate properties corresponding to decors, since they are func. dep. on the group-by variables.
    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        GroupByOperator gby = (GroupByOperator) op;
        ILogicalOperator childOp = gby.getInputs().get(0).getValue();
        IPhysicalPropertiesVector childProperties = childOp.getDeliveredPhysicalProperties();
        IPartitioningProperty partitioning =
                computePartitioningProperty(gby, childProperties.getPartitioningProperty());
        List<ILocalStructuralProperty> local = computeLocalProperties(gby, childProperties.getLocalProperties());
        deliveredProperties = new StructuralPropertiesVector(partitioning, local);
    }

    private IPartitioningProperty computePartitioningProperty(GroupByOperator gby,
            IPartitioningProperty childPartitioning) {
        Map<LogicalVariable, LogicalVariable> substMap =
                computePartitioningPropertySubstitutionMap(gby, childPartitioning);
        return substMap != null ? childPartitioning.substituteColumnVars(substMap) : childPartitioning;
    }

    private List<ILocalStructuralProperty> computeLocalProperties(GroupByOperator gby,
            List<ILocalStructuralProperty> childLocals) {
        List<ILocalStructuralProperty> propsLocal = new ArrayList<>();
        if (childLocals != null) {
            for (ILocalStructuralProperty lsp : childLocals) {
                ILocalStructuralProperty propagatedLsp = getPropagatedProperty(lsp, gby);
                if (propagatedLsp != null) {
                    propsLocal.add(propagatedLsp);
                }
            }
        }
        return propsLocal;
    }

    // If we have "gby var1 as var3, var2 as var4"
    // and input is partitioned on (var1,var2) then output is partitioned on (var3,var4)
    private Map<LogicalVariable, LogicalVariable> computePartitioningPropertySubstitutionMap(GroupByOperator gbyOp,
            IPartitioningProperty childpp) {
        Set<LogicalVariable> childPartitioningColumns = new HashSet<>();
        childpp.getColumns(childPartitioningColumns);

        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList = gbyOp.getGroupByList();
        if (groupByList.size() != childPartitioningColumns.size()) {
            return null;
        }

        Map<LogicalVariable, LogicalVariable> substMap = null;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : groupByList) {
            ILogicalExpression expr = ve.second.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return null;
            }
            VariableReferenceExpression varRefExpr = (VariableReferenceExpression) expr;
            LogicalVariable var = varRefExpr.getVariableReference();
            if (!childPartitioningColumns.remove(var)) {
                return null;
            }
            if (substMap == null) {
                substMap = new HashMap<>();
            }
            substMap.put(var, ve.first);
        }

        return childPartitioningColumns.isEmpty() ? substMap : null;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        GroupByOperator gby = (GroupByOperator) op;
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
        if (gby.isGroupAll() && gby.isGlobal()) {
            if (op.getExecutionMode() == ExecutionMode.UNPARTITIONED) {
                pv[0] = new StructuralPropertiesVector(IPartitioningProperty.UNPARTITIONED, null);
                return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
            } else {
                return emptyUnaryRequirements();
            }
        }

        List<ILocalStructuralProperty> localProps = new ArrayList<>();
        Set<LogicalVariable> gbvars = new ListSet<>(columnList);
        LocalGroupingProperty groupProp = new LocalGroupingProperty(gbvars, new ArrayList<>(columnList));

        boolean goon = true;
        for (ILogicalPlan p : gby.getNestedPlans()) {
            // try to propagate secondary order requirements from nested
            // groupings
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                AbstractLogicalOperator op1 = (AbstractLogicalOperator) r.getValue();
                if (op1.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                    AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
                    IPhysicalOperator pop2 = op2.getPhysicalOperator();
                    if (pop2 instanceof AbstractPreclusteredGroupByPOperator) {
                        List<LogicalVariable> gbyColumns =
                                ((AbstractPreclusteredGroupByPOperator) pop2).getGroupByColumns();
                        List<LogicalVariable> sndOrder = new ArrayList<>();
                        sndOrder.addAll(gbyColumns);
                        Set<LogicalVariable> freeVars = new HashSet<>();
                        try {
                            OperatorPropertiesUtil.getFreeVariablesInSelfOrDesc(op2, freeVars);
                        } catch (AlgebricksException e) {
                            throw new IllegalStateException(e);
                        }
                        // Only considers group key variables defined out-side the outer-most group-by operator.
                        sndOrder.retainAll(freeVars);
                        groupProp.getColumnSet().addAll(sndOrder);
                        groupProp.getPreferredOrderEnforcer().addAll(sndOrder);
                        goon = false;
                        break;
                    }
                }
            }
            if (!goon) {
                break;
            }
        }

        localProps.add(groupProp);

        if (reqdByParent != null) {
            // propagate parent requirements
            List<ILocalStructuralProperty> lpPar = reqdByParent.getLocalProperties();
            if (lpPar != null) {
                boolean allOk = true;
                List<ILocalStructuralProperty> props = new ArrayList<>(lpPar.size());
                for (ILocalStructuralProperty prop : lpPar) {
                    if (prop.getPropertyType() != PropertyType.LOCAL_ORDER_PROPERTY) {
                        allOk = false;
                        break;
                    }
                    LocalOrderProperty lop = (LocalOrderProperty) prop;
                    List<OrderColumn> orderColumns = new ArrayList<>();
                    List<OrderColumn> ords = lop.getOrderColumns();
                    for (OrderColumn ord : ords) {
                        Pair<LogicalVariable, Mutable<ILogicalExpression>> p = getGbyPairByRhsVar(gby, ord.getColumn());
                        if (p == null) {
                            p = getDecorPairByRhsVar(gby, ord.getColumn());
                            if (p == null) {
                                allOk = false;
                                break;
                            }
                        }
                        ILogicalExpression e = p.second.getValue();
                        if (e.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                            throw new IllegalStateException(
                                    "Right hand side of group-by assignment should have been normalized to a variable reference.");
                        }
                        LogicalVariable v = ((VariableReferenceExpression) e).getVariableReference();
                        orderColumns.add(new OrderColumn(v, ord.getOrder()));
                    }
                    props.add(new LocalOrderProperty(orderColumns));
                }
                List<FunctionalDependency> fdList = new ArrayList<>();
                for (Pair<LogicalVariable, Mutable<ILogicalExpression>> decorPair : gby.getDecorList()) {
                    List<LogicalVariable> hd = gby.getGroupByVarList();
                    List<LogicalVariable> tl = new ArrayList<>();
                    tl.add(((VariableReferenceExpression) decorPair.second.getValue()).getVariableReference());
                    fdList.add(new FunctionalDependency(hd, tl));
                }
                if (allOk && PropertiesUtil.matchLocalProperties(localProps, props, new HashMap<>(), fdList)) {
                    localProps = props;
                }
            }
        }

        IPartitioningProperty pp = null;
        AbstractLogicalOperator aop = (AbstractLogicalOperator) op;
        if (aop.getExecutionMode() == ExecutionMode.PARTITIONED) {
            pp = new UnorderedPartitionedProperty(new ListSet<>(columnList), context.getComputationNodeDomain());
        }
        pv[0] = new StructuralPropertiesVector(pp, localProps);
        return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    private static Pair<LogicalVariable, Mutable<ILogicalExpression>> getGbyPairByRhsVar(GroupByOperator gby,
            LogicalVariable var) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : gby.getGroupByList()) {
            if (ve.first == var) {
                return ve;
            }
        }
        return null;
    }

    private static Pair<LogicalVariable, Mutable<ILogicalExpression>> getDecorPairByRhsVar(GroupByOperator gby,
            LogicalVariable var) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : gby.getDecorList()) {
            if (ve.first == var) {
                return ve;
            }
        }
        return null;
    }

    private static LogicalVariable getLhsGbyVar(GroupByOperator gby, LogicalVariable var) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : gby.getGroupByList()) {
            ILogicalExpression e = ve.second.getValue();
            if (e.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new IllegalStateException(
                        "Right hand side of group by assignment should have been normalized to a variable reference.");
            }
            LogicalVariable v = ((VariableReferenceExpression) e).getVariableReference();
            if (v.equals(var)) {
                return ve.first;
            }
        }
        return null;
    }

    // Returns the local structure property that is propagated from an input local structure property
    // through a pre-clustered GROUP BY physical operator.
    private ILocalStructuralProperty getPropagatedProperty(ILocalStructuralProperty lsp, GroupByOperator gby) {
        PropertyType propertyType = lsp.getPropertyType();
        if (propertyType == PropertyType.LOCAL_GROUPING_PROPERTY) {
            // A new grouping property is generated.
            return new LocalGroupingProperty(new ListSet<>(gby.getGroupByVarList()));
        } else {
            LocalOrderProperty lop = (LocalOrderProperty) lsp;
            List<OrderColumn> orderColumns = new ArrayList<>();
            for (OrderColumn oc : lop.getOrderColumns()) {
                LogicalVariable v2 = getLhsGbyVar(gby, oc.getColumn());
                if (v2 != null) {
                    orderColumns.add(new OrderColumn(v2, oc.getOrder()));
                } else {
                    break;
                }
            }
            // Only the prefix (regarding to the pre-clustered GROUP BY keys) of the ordering property can be
            // maintained.
            return orderColumns.isEmpty() ? null : new LocalOrderProperty(orderColumns);
        }
    }
}
