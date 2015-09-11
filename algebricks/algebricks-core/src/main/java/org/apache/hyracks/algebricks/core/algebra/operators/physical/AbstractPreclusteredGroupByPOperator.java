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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
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

public abstract class AbstractPreclusteredGroupByPOperator extends AbstractPhysicalOperator {

    protected List<LogicalVariable> columnList;

    public AbstractPreclusteredGroupByPOperator(List<LogicalVariable> columnList) {
        this.columnList = columnList;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + columnList;
    }

    public List<LogicalVariable> getGbyColumns() {
        return columnList;
    }

    public void setGbyColumns(List<LogicalVariable> gByColumns) {
        this.columnList = gByColumns;
    }

    // Obs: We don't propagate properties corresponding to decors, since they
    // are func. dep. on the group-by variables.
    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        List<ILocalStructuralProperty> propsLocal = new LinkedList<ILocalStructuralProperty>();
        GroupByOperator gby = (GroupByOperator) op;
        ILogicalOperator op2 = gby.getInputs().get(0).getValue();
        IPhysicalPropertiesVector childProp = op2.getDeliveredPhysicalProperties();
        IPartitioningProperty pp = childProp.getPartitioningProperty();
        List<ILocalStructuralProperty> childLocals = childProp.getLocalProperties();
        if (childLocals != null) {
            for (ILocalStructuralProperty lsp : childLocals) {
                boolean failed = false;
                switch (lsp.getPropertyType()) {
                    case LOCAL_GROUPING_PROPERTY: {
                        LocalGroupingProperty lgp = (LocalGroupingProperty) lsp;
                        Set<LogicalVariable> colSet = new ListSet<LogicalVariable>();
                        for (LogicalVariable v : lgp.getColumnSet()) {
                            LogicalVariable v2 = getLhsGbyVar(gby, v);
                            if (v2 != null) {
                                colSet.add(v2);
                            } else {
                                failed = true;
                            }
                        }
                        if (!failed) {
                            propsLocal.add(new LocalGroupingProperty(colSet));
                        }
                        break;
                    }
                    case LOCAL_ORDER_PROPERTY: {
                        LocalOrderProperty lop = (LocalOrderProperty) lsp;
                        List<OrderColumn> orderColumns = new ArrayList<OrderColumn>();
                        for (OrderColumn oc : lop.getOrderColumns()) {
                            LogicalVariable v2 = getLhsGbyVar(gby, oc.getColumn());
                            if (v2 != null) {
                                orderColumns.add(new OrderColumn(v2, oc.getOrder()));
                            } else {
                                failed = true;
                            }
                        }
                        if (!failed) {
                            propsLocal.add(new LocalOrderProperty(orderColumns));
                        }
                        break;
                    }
                    default: {
                        throw new IllegalStateException();
                    }
                }
                if (failed) {
                    break;
                }
            }
        }
        deliveredProperties = new StructuralPropertiesVector(pp, propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
        List<ILocalStructuralProperty> localProps = null;

        localProps = new ArrayList<ILocalStructuralProperty>(1);
        Set<LogicalVariable> gbvars = new ListSet<LogicalVariable>(columnList);
        LocalGroupingProperty groupProp = new LocalGroupingProperty(gbvars, new ArrayList<LogicalVariable>(columnList));

        GroupByOperator gby = (GroupByOperator) op;
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
                        List<LogicalVariable> sndOrder = ((AbstractPreclusteredGroupByPOperator) pop2).getGbyColumns();
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
                List<ILocalStructuralProperty> props = new ArrayList<ILocalStructuralProperty>(lpPar.size());
                for (ILocalStructuralProperty prop : lpPar) {
                    if (prop.getPropertyType() != PropertyType.LOCAL_ORDER_PROPERTY) {
                        allOk = false;
                        break;
                    }
                    LocalOrderProperty lop = (LocalOrderProperty) prop;
                    List<OrderColumn> orderColumns = new ArrayList<OrderColumn>();
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
                List<FunctionalDependency> fdList = new ArrayList<FunctionalDependency>();
                for (Pair<LogicalVariable, Mutable<ILogicalExpression>> decorPair : gby.getDecorList()) {
                    List<LogicalVariable> hd = gby.getGbyVarList();
                    List<LogicalVariable> tl = new ArrayList<LogicalVariable>(1);
                    tl.add(((VariableReferenceExpression) decorPair.second.getValue()).getVariableReference());
                    fdList.add(new FunctionalDependency(hd, tl));
                }
                if (allOk
                        && PropertiesUtil.matchLocalProperties(localProps, props,
                                new HashMap<LogicalVariable, EquivalenceClass>(), fdList)) {
                    localProps = props;
                }
            }
        }

        IPartitioningProperty pp = null;
        AbstractLogicalOperator aop = (AbstractLogicalOperator) op;
        if (aop.getExecutionMode() == ExecutionMode.PARTITIONED) {
            pp = new UnorderedPartitionedProperty(new ListSet<LogicalVariable>(columnList), null);
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
            if (v == var) {
                return ve.first;
            }
        }
        return null;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }
}
