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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.FDsAndEquivClassesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractPreSortedDistinctByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractStableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.BroadcastExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionMergeExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.MicroStableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomMergeExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RangePartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SequentialMergeExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortForwardPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortMergeExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty.PartitioningType;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalGroupingProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.PropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.util.LogRedactionUtil;

public class EnforceStructuralPropertiesRule implements IAlgebraicRewriteRule {

    private static final String HASH_MERGE = "hash_merge";
    private static final String TRUE_CONSTANT = "true";
    private final FunctionIdentifier rangeMapFunction;
    private final FunctionIdentifier localSamplingFun;
    private final FunctionIdentifier typePropagatingFun;

    public EnforceStructuralPropertiesRule(FunctionIdentifier rangeMapFunction, FunctionIdentifier localSamplingFun,
            FunctionIdentifier typePropagatingFun) {
        this.rangeMapFunction = rangeMapFunction;
        this.localSamplingFun = localSamplingFun;
        this.typePropagatingFun = typePropagatingFun;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        // wait for the physical operators to be set first
        if (op.getPhysicalOperator() == null) {
            return false;
        }
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        List<FunctionalDependency> fds = context.getFDList(op);
        if (fds != null && !fds.isEmpty()) {
            return false;
        }
        // These are actually logical constraints, so they could be pre-computed
        // somewhere else, too.

        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">>>> Optimizing operator " + op.getPhysicalOperator() + ".\n");
        }

        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(op, context);

        StructuralPropertiesVector pvector =
                new StructuralPropertiesVector(new RandomPartitioningProperty(context.getComputationNodeDomain()),
                        new LinkedList<ILocalStructuralProperty>());
        boolean changed = physOptimizeOp(opRef, pvector, false, context);
        op.computeDeliveredPhysicalProperties(context);
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">>>> Structural properties for " + op.getPhysicalOperator() + ": "
                    + op.getDeliveredPhysicalProperties() + "\n");
        }

        context.addToDontApplySet(this, opRef.getValue());

        return changed;
    }

    private boolean physOptimizeOp(Mutable<ILogicalOperator> opRef, IPhysicalPropertiesVector required,
            boolean nestedPlan, IOptimizationContext context) throws AlgebricksException {

        boolean changed = false;
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        optimizeUsingConstraintsAndEquivClasses(op);
        PhysicalRequirements pr = op.getRequiredPhysicalPropertiesForChildren(required, context);
        IPhysicalPropertiesVector[] reqdProperties = null;
        if (pr != null) {
            reqdProperties = pr.getRequiredProperties();
        }
        boolean opIsRedundantSort = false;

        // compute properties and figure out the domain
        INodeDomain childrenDomain = null;
        int j = 0;
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            AbstractLogicalOperator child = (AbstractLogicalOperator) childRef.getValue();
            changed |= physOptimizeOp(childRef, reqdProperties[j], nestedPlan, context);
            child.computeDeliveredPhysicalProperties(context);
            IPhysicalPropertiesVector delivered = child.getDeliveredPhysicalProperties();
            INodeDomain childDomain = delivered.getPartitioningProperty().getNodeDomain();
            if (childrenDomain == null) {
                childrenDomain = delivered.getPartitioningProperty().getNodeDomain();
            } else if (!childrenDomain.sameAs(childDomain)) {
                childrenDomain = context.getComputationNodeDomain();
            }
            j++;
        }

        if (reqdProperties != null) {
            for (int k = 0; k < reqdProperties.length; k++) {
                IPhysicalPropertiesVector pv = reqdProperties[k];
                IPartitioningProperty pp = pv.getPartitioningProperty();
                if (pp != null && pp.getNodeDomain() == null) {
                    pp.setNodeDomain(childrenDomain);
                }
            }
        }

        boolean loggerTraceEnabled = AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled();

        // The child index of the child operator to optimize first.
        int startChildIndex = getStartChildIndex(op, pr, nestedPlan);
        IPartitioningProperty firstDeliveredPartitioning = null;
        // Enforce data properties in a top-down manner.
        for (j = 0; j < op.getInputs().size(); j++) {
            // Starts from a partitioning-compatible child if any to loop over all children.
            int childIndex = (j + startChildIndex) % op.getInputs().size();
            IPhysicalPropertiesVector requiredProperty = reqdProperties[childIndex];
            AbstractLogicalOperator child = (AbstractLogicalOperator) op.getInputs().get(childIndex).getValue();
            IPhysicalPropertiesVector delivered = child.getDeliveredPhysicalProperties();

            if (loggerTraceEnabled) {
                AlgebricksConfig.ALGEBRICKS_LOGGER
                        .trace(">>>> Properties delivered by " + child.getPhysicalOperator() + ": " + delivered + "\n");
            }
            IPartitioningRequirementsCoordinator prc = pr.getPartitioningCoordinator();
            // Coordinates requirements by looking at the firstDeliveredPartitioning.
            Pair<Boolean, IPartitioningProperty> pbpp = prc.coordinateRequirements(
                    requiredProperty.getPartitioningProperty(), firstDeliveredPartitioning, op, context);
            boolean mayExpandPartitioningProperties = pbpp.first;
            IPhysicalPropertiesVector rqd =
                    new StructuralPropertiesVector(pbpp.second, requiredProperty.getLocalProperties());

            if (loggerTraceEnabled) {
                AlgebricksConfig.ALGEBRICKS_LOGGER
                        .trace(">>>> Required properties for " + child.getPhysicalOperator() + ": " + rqd + "\n");
            }
            // The partitioning property of reqdProperties[childIndex] could be updated here because
            // rqd.getPartitioningProperty() is the same object instance as requiredProperty.getPartitioningProperty().
            IPhysicalPropertiesVector diff = delivered.getUnsatisfiedPropertiesFrom(rqd,
                    mayExpandPartitioningProperties, context.getEquivalenceClassMap(child), context.getFDList(child));

            if (isRedundantSort(opRef, delivered, diff, context)) {
                opIsRedundantSort = true;
            }

            if (diff != null) {
                changed = true;
                addEnforcers(op, childIndex, diff, rqd, delivered, childrenDomain, nestedPlan, context);

                AbstractLogicalOperator newChild = (AbstractLogicalOperator) op.getInputs().get(childIndex).getValue();

                if (newChild != child) {
                    delivered = newChild.getDeliveredPhysicalProperties();
                    IPhysicalPropertiesVector newDiff =
                            newPropertiesDiff(newChild, rqd, mayExpandPartitioningProperties, context);
                    if (loggerTraceEnabled) {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">>>> New properties diff: " + newDiff + "\n");
                    }

                    if (isRedundantSort(opRef, delivered, newDiff, context)) {
                        opIsRedundantSort = true;
                        break;
                    }
                }
            }

            if (firstDeliveredPartitioning == null) {
                firstDeliveredPartitioning = delivered.getPartitioningProperty();
            }
        }

        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans nested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : nested.getNestedPlans()) {
                if (physOptimizePlan(p, required, true, context)) {
                    changed = true;
                }
            }
        }

        if (opIsRedundantSort) {
            if (loggerTraceEnabled) {
                AlgebricksConfig.ALGEBRICKS_LOGGER
                        .trace(">>>> Removing redundant SORT operator " + op.getPhysicalOperator() + "\n");
                printOp(op, context);
            }
            changed = true;
            AbstractLogicalOperator nextOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            if (nextOp.getOperatorTag() == LogicalOperatorTag.PROJECT) {
                nextOp = (AbstractLogicalOperator) nextOp.getInputs().get(0).getValue();
            }
            opRef.setValue(nextOp);
            // Now, transfer annotations from the original sort op. to this one.
            AbstractLogicalOperator transferTo = nextOp;
            if (transferTo.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                // remove duplicate exchange operator
                transferTo = (AbstractLogicalOperator) transferTo.getInputs().get(0).getValue();
            }
            transferTo.getAnnotations().putAll(op.getAnnotations());
            physOptimizeOp(opRef, required, nestedPlan, context);
        }
        return changed;
    }

    private boolean physOptimizePlan(ILogicalPlan plan, IPhysicalPropertiesVector pvector, boolean nestedPlan,
            IOptimizationContext context) throws AlgebricksException {
        boolean loggerTraceEnabled = AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled();
        boolean changed = false;
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            if (physOptimizeOp(root, pvector, nestedPlan, context)) {
                changed = true;
            }
            AbstractLogicalOperator op = (AbstractLogicalOperator) root.getValue();
            op.computeDeliveredPhysicalProperties(context);
            if (loggerTraceEnabled) {
                AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">>>> Structural properties for " + op.getPhysicalOperator()
                        + ": " + op.getDeliveredPhysicalProperties() + "\n");
            }
        }
        return changed;
    }

    // Gets the index of a child to start top-down data property enforcement.
    // If there is a partitioning-compatible child with the operator in opRef,
    // start from this child; otherwise, start from child zero.
    private int getStartChildIndex(AbstractLogicalOperator op, PhysicalRequirements pr, boolean nestedPlan) {
        IPhysicalPropertiesVector[] reqdProperties = null;
        if (pr != null) {
            reqdProperties = pr.getRequiredProperties();
        }

        List<IPartitioningProperty> deliveredPartitioningPropertiesFromChildren = new ArrayList<>();
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            AbstractLogicalOperator child = (AbstractLogicalOperator) childRef.getValue();
            deliveredPartitioningPropertiesFromChildren
                    .add(child.getDeliveredPhysicalProperties().getPartitioningProperty());
        }
        int partitioningCompatibleChild = 0;
        for (int i = 0; i < op.getInputs().size(); i++) {
            IPartitioningProperty deliveredPropertyFromChild = deliveredPartitioningPropertiesFromChildren.get(i);
            if (reqdProperties == null || reqdProperties[i] == null
                    || reqdProperties[i].getPartitioningProperty() == null || deliveredPropertyFromChild == null
                    || reqdProperties[i].getPartitioningProperty()
                            .getPartitioningType() != deliveredPartitioningPropertiesFromChildren.get(i)
                                    .getPartitioningType()) {
                continue;
            }
            IPartitioningProperty requiredPropertyForChild = reqdProperties[i].getPartitioningProperty();
            // If child i's delivered partitioning property already satisfies the required property, stop and return the child index.
            if (PropertiesUtil.matchPartitioningProps(requiredPropertyForChild, deliveredPropertyFromChild, true)) {
                partitioningCompatibleChild = i;
                break;
            }
        }
        return partitioningCompatibleChild;
    }

    private IPhysicalPropertiesVector newPropertiesDiff(AbstractLogicalOperator newChild,
            IPhysicalPropertiesVector required, boolean mayExpandPartitioningProperties, IOptimizationContext context)
            throws AlgebricksException {
        IPhysicalPropertiesVector newDelivered = newChild.getDeliveredPhysicalProperties();

        Map<LogicalVariable, EquivalenceClass> newChildEqClasses = context.getEquivalenceClassMap(newChild);
        List<FunctionalDependency> newChildFDs = context.getFDList(newChild);
        if (newChildEqClasses == null || newChildFDs == null) {
            FDsAndEquivClassesVisitor fdsVisitor = new FDsAndEquivClassesVisitor();
            newChild.accept(fdsVisitor, context);
            newChildEqClasses = context.getEquivalenceClassMap(newChild);
            newChildFDs = context.getFDList(newChild);
        }
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(
                    ">>>> Required properties for new op. " + newChild.getPhysicalOperator() + ": " + required + "\n");
        }

        return newDelivered.getUnsatisfiedPropertiesFrom(required, mayExpandPartitioningProperties, newChildEqClasses,
                newChildFDs);
    }

    private void optimizeUsingConstraintsAndEquivClasses(AbstractLogicalOperator op) {
        IPhysicalOperator pOp = op.getPhysicalOperator();
        switch (pOp.getOperatorTag()) {
            case EXTERNAL_GROUP_BY:
            case PRE_CLUSTERED_GROUP_BY:
            case MICRO_PRE_CLUSTERED_GROUP_BY: {
                GroupByOperator gby = (GroupByOperator) op;
                AbstractGroupByPOperator gbyPhysOp = (AbstractGroupByPOperator) pOp;
                gbyPhysOp.setGroupByColumns(gby.getGroupByVarList());
                break;
            }
            case PRE_SORTED_DISTINCT_BY:
            case MICRO_PRE_SORTED_DISTINCT_BY: {
                DistinctOperator d = (DistinctOperator) op;
                AbstractPreSortedDistinctByPOperator preSortedDistinct = (AbstractPreSortedDistinctByPOperator) pOp;
                preSortedDistinct.setDistinctByColumns(d.getDistinctByVarList());
                break;
            }
            default:
        }
    }

    private List<OrderColumn> getOrderColumnsFromGroupingProperties(List<ILocalStructuralProperty> reqd,
            List<ILocalStructuralProperty> dlvd) {
        List<OrderColumn> returnedProperties = new ArrayList<>();
        List<LogicalVariable> rqdCols = new ArrayList<>();
        List<LogicalVariable> dlvdCols = new ArrayList<>();
        for (ILocalStructuralProperty r : reqd) {
            r.getVariables(rqdCols);
        }
        for (ILocalStructuralProperty d : dlvd) {
            d.getVariables(dlvdCols);
        }

        int prefix = dlvdCols.size() - 1;
        while (prefix >= 0) {
            if (!rqdCols.contains(dlvdCols.get(prefix))) {
                prefix--;
            } else {
                break;
            }
        }

        LocalOrderProperty orderProp = (LocalOrderProperty) dlvd.get(0);
        List<OrderColumn> orderColumns = orderProp.getOrderColumns();
        for (int j = 0; j <= prefix; j++) {
            returnedProperties.add(new OrderColumn(orderColumns.get(j).getColumn(), orderColumns.get(j).getOrder()));
        }
        // maintain other order columns after the required order columns
        if (!returnedProperties.isEmpty()) {
            for (int j = prefix + 1; j < dlvdCols.size(); j++) {
                OrderColumn oc = orderColumns.get(j);
                returnedProperties.add(new OrderColumn(oc.getColumn(), oc.getOrder()));
            }
        }
        return returnedProperties;
    }

    /*
     * We assume delivered to be already normalized.
     */
    private boolean isRedundantSort(Mutable<ILogicalOperator> opRef, IPhysicalPropertiesVector delivered,
            IPhysicalPropertiesVector diffOfProperties, IOptimizationContext context) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ORDER
                || (op.getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.STABLE_SORT
                        && op.getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.MICRO_STABLE_SORT)
                || delivered.getLocalProperties() == null) {
            return false;
        }
        AbstractStableSortPOperator sortOp = (AbstractStableSortPOperator) op.getPhysicalOperator();
        sortOp.computeLocalProperties((OrderOperator) op);
        ILocalStructuralProperty orderProp = sortOp.getOrderProperty();
        return PropertiesUtil.matchLocalProperties(Collections.singletonList(orderProp), delivered.getLocalProperties(),
                context.getEquivalenceClassMap(op), context.getFDList(op));
    }

    private void addEnforcers(AbstractLogicalOperator op, int childIndex,
            IPhysicalPropertiesVector diffPropertiesVector, IPhysicalPropertiesVector required,
            IPhysicalPropertiesVector deliveredByChild, INodeDomain domain, boolean nestedPlan,
            IOptimizationContext context) throws AlgebricksException {
        IPartitioningProperty pp = diffPropertiesVector.getPartitioningProperty();
        if (pp == null || pp.getPartitioningType() == PartitioningType.UNPARTITIONED) {
            addLocalEnforcers(op, childIndex, diffPropertiesVector.getLocalProperties(), nestedPlan, context);
            IPhysicalPropertiesVector deliveredByNewChild =
                    op.getInputs().get(0).getValue().getDeliveredPhysicalProperties();
            if (!nestedPlan) {
                addPartitioningEnforcers(op, childIndex, pp, required, deliveredByNewChild, domain, context);
            }
        } else {
            if (!nestedPlan) {
                addPartitioningEnforcers(op, childIndex, pp, required, deliveredByChild, pp.getNodeDomain(), context);
            }
            AbstractLogicalOperator newChild = (AbstractLogicalOperator) op.getInputs().get(childIndex).getValue();
            IPhysicalPropertiesVector newDiff = newPropertiesDiff(newChild, required, true, context);
            if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
                AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">>>> New properties diff: " + newDiff + "\n");
            }
            if (newDiff != null) {
                addLocalEnforcers(op, childIndex, newDiff.getLocalProperties(), nestedPlan, context);
            }
        }
    }

    private void addLocalEnforcers(AbstractLogicalOperator op, int i, List<ILocalStructuralProperty> localProperties,
            boolean nestedPlan, IOptimizationContext context) throws AlgebricksException {
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER
                    .trace(">>>> Adding local enforcers for local props = " + localProperties + "\n");
        }

        if (localProperties == null || localProperties.isEmpty()) {
            return;
        }

        Mutable<ILogicalOperator> topOp = new MutableObject<>();
        topOp.setValue(op.getInputs().get(i).getValue());
        LinkedList<LocalOrderProperty> oList = new LinkedList<>();

        for (ILocalStructuralProperty prop : localProperties) {
            switch (prop.getPropertyType()) {
                case LOCAL_ORDER_PROPERTY: {
                    oList.add((LocalOrderProperty) prop);
                    break;
                }
                case LOCAL_GROUPING_PROPERTY: {
                    LocalGroupingProperty g = (LocalGroupingProperty) prop;
                    Collection<LogicalVariable> vars =
                            !g.getPreferredOrderEnforcer().isEmpty() ? g.getPreferredOrderEnforcer() : g.getColumnSet();
                    List<OrderColumn> orderColumns = new ArrayList<>();
                    for (LogicalVariable v : vars) {
                        OrderColumn oc = new OrderColumn(v, OrderKind.ASC);
                        orderColumns.add(oc);
                    }
                    LocalOrderProperty lop = new LocalOrderProperty(orderColumns);
                    oList.add(lop);
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
        }
        if (!oList.isEmpty()) {
            topOp = enforceOrderProperties(oList, topOp, nestedPlan, context);
        }

        op.getInputs().set(i, topOp);
        OperatorPropertiesUtil.computeSchemaAndPropertiesRecIfNull((AbstractLogicalOperator) topOp.getValue(), context);
        OperatorManipulationUtil.setOperatorMode(op);
        printOp((AbstractLogicalOperator) topOp.getValue(), context);
    }

    private Mutable<ILogicalOperator> enforceOrderProperties(List<LocalOrderProperty> oList,
            Mutable<ILogicalOperator> topOp, boolean isMicroOp, IOptimizationContext context)
            throws AlgebricksException {
        SourceLocation sourceLoc = topOp.getValue().getSourceLocation();
        List<Pair<IOrder, Mutable<ILogicalExpression>>> oe = new LinkedList<>();
        for (LocalOrderProperty orderProperty : oList) {
            for (OrderColumn oc : orderProperty.getOrderColumns()) {
                IOrder ordType = (oc.getOrder() == OrderKind.ASC) ? OrderOperator.ASC_ORDER : OrderOperator.DESC_ORDER;
                VariableReferenceExpression ocColumnRef = new VariableReferenceExpression(oc.getColumn());
                ocColumnRef.setSourceLocation(sourceLoc);
                Pair<IOrder, Mutable<ILogicalExpression>> pair =
                        new Pair<>(ordType, new MutableObject<ILogicalExpression>(ocColumnRef));
                oe.add(pair);
            }
        }
        OrderOperator oo = new OrderOperator(oe);
        oo.setSourceLocation(sourceLoc);
        oo.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
        if (isMicroOp) {
            oo.setPhysicalOperator(new MicroStableSortPOperator());
        } else {
            oo.setPhysicalOperator(new StableSortPOperator());
        }
        oo.getInputs().add(topOp);
        context.computeAndSetTypeEnvironmentForOperator(oo);
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">>>> Added sort enforcer " + oo.getPhysicalOperator() + ".\n");
        }
        return new MutableObject<ILogicalOperator>(oo);
    }

    /**
     * Adds exchange operators (connectors) between {@code op} & its child at index {@code childIdx}.
     * @param op the parent operator that is requiring a specific kind of connector at its child
     * @param i the child index where we want to have the connector
     * @param pp the required partitioning property at that child (i.e. the required connector)
     * @param required the physical properties required at that child (partitioning + local properties)
     * @param deliveredByChild the physical properties delivered by that child (partitioning + local properties)
     * @param domain the destination domain of nodes that we want the connector to connect to
     * @param context {@link IOptimizationContext}
     * @throws AlgebricksException
     */
    private void addPartitioningEnforcers(ILogicalOperator op, int i, IPartitioningProperty pp,
            IPhysicalPropertiesVector required, IPhysicalPropertiesVector deliveredByChild, INodeDomain domain,
            IOptimizationContext context) throws AlgebricksException {
        if (pp != null) {
            IPhysicalOperator pop;
            switch (pp.getPartitioningType()) {
                case UNPARTITIONED: {
                    pop = createMergingConnector(deliveredByChild);
                    break;
                }
                case UNORDERED_PARTITIONED: {
                    pop = createHashConnector(context, deliveredByChild, domain, required, pp, i, op);
                    break;
                }
                case ORDERED_PARTITIONED: {
                    pop = createRangePartitionerConnector((AbstractLogicalOperator) op, domain, pp, i, context);
                    break;
                }
                case BROADCAST: {
                    pop = new BroadcastExchangePOperator(domain);
                    break;
                }
                case RANDOM: {
                    RandomPartitioningProperty rpp = (RandomPartitioningProperty) pp;
                    INodeDomain nd = rpp.getNodeDomain();
                    pop = new RandomPartitionExchangePOperator(nd);
                    break;
                }
                default: {
                    throw new NotImplementedException("Enforcer for " + pp.getPartitioningType()
                            + " partitioning type has not been implemented.");
                }
            }
            Mutable<ILogicalOperator> ci = op.getInputs().get(i);
            ExchangeOperator exchg = new ExchangeOperator();
            exchg.setPhysicalOperator(pop);
            setNewOp(ci, exchg, context);
            exchg.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
            OperatorPropertiesUtil.computeSchemaAndPropertiesRecIfNull(exchg, context);
            context.computeAndSetTypeEnvironmentForOperator(exchg);
            if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
                AlgebricksConfig.ALGEBRICKS_LOGGER
                        .trace(">>>> Added partitioning enforcer " + exchg.getPhysicalOperator() + ".\n");
                printOp((AbstractLogicalOperator) op, context);
            }
        }
    }

    private IPhysicalOperator createMergingConnector(IPhysicalPropertiesVector deliveredByChild) {
        List<OrderColumn> ordCols = computeOrderColumns(deliveredByChild);
        if (ordCols.isEmpty()) {
            IPartitioningProperty partitioningDeliveredByChild = deliveredByChild.getPartitioningProperty();
            if (partitioningDeliveredByChild.getPartitioningType() == PartitioningType.ORDERED_PARTITIONED) {
                return new SequentialMergeExchangePOperator();
            } else {
                return new RandomMergeExchangePOperator();
            }
        } else {
            return new SortMergeExchangePOperator(ordCols.toArray(new OrderColumn[ordCols.size()]));
        }
    }

    private IPhysicalOperator createHashConnector(IOptimizationContext ctx, IPhysicalPropertiesVector deliveredByChild,
            INodeDomain domain, IPhysicalPropertiesVector requiredAtChild, IPartitioningProperty rqdPartitioning,
            int childIndex, ILogicalOperator parentOp) {
        IPhysicalOperator hashConnector;
        List<LogicalVariable> vars = new ArrayList<>(((UnorderedPartitionedProperty) rqdPartitioning).getColumnSet());
        String hashMergeHint = (String) ctx.getMetadataProvider().getConfig().get(HASH_MERGE);
        if (hashMergeHint == null || !hashMergeHint.equalsIgnoreCase(TRUE_CONSTANT)) {
            hashConnector = new HashPartitionExchangePOperator(vars, domain);
            return hashConnector;
        }
        List<ILocalStructuralProperty> cldLocals = deliveredByChild.getLocalProperties();
        List<ILocalStructuralProperty> reqdLocals = requiredAtChild.getLocalProperties();
        boolean propWasSet = false;
        hashConnector = null;
        if (reqdLocals != null && cldLocals != null && allAreOrderProps(cldLocals)) {
            AbstractLogicalOperator c = (AbstractLogicalOperator) parentOp.getInputs().get(childIndex).getValue();
            Map<LogicalVariable, EquivalenceClass> ecs = ctx.getEquivalenceClassMap(c);
            List<FunctionalDependency> fds = ctx.getFDList(c);
            if (PropertiesUtil.matchLocalProperties(reqdLocals, cldLocals, ecs, fds)) {
                List<OrderColumn> orderColumns = getOrderColumnsFromGroupingProperties(reqdLocals, cldLocals);
                hashConnector = new HashPartitionMergeExchangePOperator(orderColumns, vars, domain);
                propWasSet = true;
            }
        }
        if (!propWasSet) {
            hashConnector = new HashPartitionExchangePOperator(vars, domain);
        }
        return hashConnector;
    }

    /**
     * Creates a range-based exchange operator.
     * @param parentOp the operator requiring range-based partitioner to have input tuples repartitioned using a range
     * @param domain the target node domain of the range-based partitioner
     * @param requiredPartitioning {@see OrderedPartitionedProperty}
     * @param childIndex the index of the child at which the required partitioning is needed
     * @param ctx optimization context
     * @return a range-based exchange operator
     * @throws AlgebricksException
     */
    private IPhysicalOperator createRangePartitionerConnector(AbstractLogicalOperator parentOp, INodeDomain domain,
            IPartitioningProperty requiredPartitioning, int childIndex, IOptimizationContext ctx)
            throws AlgebricksException {
        // options for range partitioning: 1. static range map, 2. dynamic range map computed at run time
        List<OrderColumn> partitioningColumns = ((OrderedPartitionedProperty) requiredPartitioning).getOrderColumns();
        if (parentOp.getAnnotations().containsKey(OperatorAnnotations.USE_STATIC_RANGE)) {
            RangeMap rangeMap = (RangeMap) parentOp.getAnnotations().get(OperatorAnnotations.USE_STATIC_RANGE);
            return new RangePartitionExchangePOperator(partitioningColumns, domain, rangeMap);
        } else {
            return createDynamicRangePartitionExchangePOperator(parentOp, ctx, domain, partitioningColumns, childIndex);
        }
    }

    private IPhysicalOperator createDynamicRangePartitionExchangePOperator(AbstractLogicalOperator parentOp,
            IOptimizationContext ctx, INodeDomain targetDomain, List<OrderColumn> partitioningColumns, int childIndex)
            throws AlgebricksException {
        SourceLocation srcLoc = parentOp.getSourceLocation();
        // #1. create the replicate operator and add it above the source op feeding parent operator
        ReplicateOperator replicateOp = createReplicateOperator(parentOp.getInputs().get(childIndex), ctx, srcLoc);

        // these two exchange ops are needed so that the parents of replicate stay the same during later optimizations.
        // This is because replicate operator has references to its parents. If any later optimizations add new parents,
        // then replicate would still point to the old ones.
        MutableObject<ILogicalOperator> replicateOpRef = new MutableObject<>(replicateOp);
        ExchangeOperator exchToLocalAgg = createOneToOneExchangeOp(replicateOpRef, ctx);
        ExchangeOperator exchToForward = createOneToOneExchangeOp(replicateOpRef, ctx);
        MutableObject<ILogicalOperator> exchToLocalAggRef = new MutableObject<>(exchToLocalAgg);
        MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchToForward);

        // add the exchange-to-forward at output 0, the exchange-to-local-aggregate at output 1
        replicateOp.getOutputs().add(exchToForwardRef);
        replicateOp.getOutputs().add(exchToLocalAggRef);
        // materialize the data to be able to re-read the data again after sampling is done
        replicateOp.getOutputMaterializationFlags()[0] = true;

        // #2. create the aggregate operators and their sampling functions
        List<LogicalVariable> localVars = new ArrayList<>();
        List<LogicalVariable> rangeMapResultVar = new ArrayList<>(1);
        List<Mutable<ILogicalExpression>> localFuns = new ArrayList<>();
        List<Mutable<ILogicalExpression>> rangeMapFun = new ArrayList<>(1);
        createAggregateFunction(ctx, localVars, localFuns, rangeMapResultVar, rangeMapFun, targetDomain.cardinality(),
                partitioningColumns, srcLoc);
        AggregateOperator localAggOp = createAggregate(localVars, false, localFuns, exchToLocalAggRef, ctx, srcLoc);
        MutableObject<ILogicalOperator> localAgg = new MutableObject<>(localAggOp);
        AggregateOperator globalAggOp = createAggregate(rangeMapResultVar, true, rangeMapFun, localAgg, ctx, srcLoc);
        MutableObject<ILogicalOperator> globalAgg = new MutableObject<>(globalAggOp);

        // #3. create the forward operator
        String rangeMapKey = UUID.randomUUID().toString();
        LogicalVariable rangeMapVar = rangeMapResultVar.get(0);
        ForwardOperator forward = createForward(rangeMapKey, rangeMapVar, exchToForwardRef, globalAgg, ctx, srcLoc);
        MutableObject<ILogicalOperator> forwardRef = new MutableObject<>(forward);

        // replace the old input of parentOp requiring the range partitioning with the new forward op
        parentOp.getInputs().set(childIndex, forwardRef);
        parentOp.recomputeSchema();
        ctx.computeAndSetTypeEnvironmentForOperator(parentOp);
        return new RangePartitionExchangePOperator(partitioningColumns, rangeMapKey, targetDomain);
    }

    private static ReplicateOperator createReplicateOperator(Mutable<ILogicalOperator> inputOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        ReplicateOperator replicateOperator = new ReplicateOperator(2);
        replicateOperator.setPhysicalOperator(new ReplicatePOperator());
        replicateOperator.setSourceLocation(sourceLocation);
        replicateOperator.getInputs().add(inputOperator);
        OperatorManipulationUtil.setOperatorMode(replicateOperator);
        replicateOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(replicateOperator);
        return replicateOperator;
    }

    /**
     * Creates the sampling expressions and embeds them in {@code localAggFunctions} & {@code globalAggFunctions}. Also,
     * creates the variables which will hold the result of each one.
     * {@code localResultVariables},{@code localAggFunctions},{@code globalResultVariables} & {@code globalAggFunctions}
     * will be used when creating the corresponding aggregate operators.
     * @param context used to get new variables which will be assigned the samples & the range map
     * @param localResultVariables the variable to which the stats (e.g. samples) info is assigned
     * @param localAggFunctions the local sampling expression and columns expressions are added to this list
     * @param globalResultVariable the variable to which the range map is assigned
     * @param globalAggFunction the expression generating a range map is added to this list
     * @param numPartitions passed to the expression generating a range map to know how many split points are needed
     * @param partitionFields the fields based on which the partitioner partitions the tuples, also sampled fields
     * @param sourceLocation source location
     */
    private void createAggregateFunction(IOptimizationContext context, List<LogicalVariable> localResultVariables,
            List<Mutable<ILogicalExpression>> localAggFunctions, List<LogicalVariable> globalResultVariable,
            List<Mutable<ILogicalExpression>> globalAggFunction, int numPartitions, List<OrderColumn> partitionFields,
            SourceLocation sourceLocation) {
        // prepare the arguments to the local sampling function: sampled fields (e.g. $col1, $col2)
        // local info: local agg [$1, $2, $3] = [local-sampling-fun($col1, $col2), type_expr($col1), type_expr($col2)]
        // global info: global agg [$RM] = [global-range-map($1, $2, $3)]
        IFunctionInfo samplingFun = context.getMetadataProvider().lookupFunction(localSamplingFun);
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(partitionFields.size());
        List<Mutable<ILogicalExpression>> argsToRM = new ArrayList<>(1 + partitionFields.size());
        AbstractFunctionCallExpression expr = new AggregateFunctionCallExpression(samplingFun, false, fields);
        expr.setSourceLocation(sourceLocation);
        expr.setOpaqueParameters(new Object[] { context.getPhysicalOptimizationConfig().getSortSamples() });
        // add the sampling function to the list of the local functions
        LogicalVariable localOutVariable = context.newVar();
        localResultVariables.add(localOutVariable);
        localAggFunctions.add(new MutableObject<>(expr));
        // add the local result variable as input to the global range map function
        AbstractLogicalExpression varExprRef = new VariableReferenceExpression(localOutVariable, sourceLocation);
        argsToRM.add(new MutableObject<>(varExprRef));
        int i = 0;
        boolean[] ascendingFlags = new boolean[partitionFields.size()];
        IFunctionInfo typeFun = context.getMetadataProvider().lookupFunction(typePropagatingFun);
        for (OrderColumn field : partitionFields) {
            // add the field to the "fields" which is the input to the local sampling function
            varExprRef = new VariableReferenceExpression(field.getColumn(), sourceLocation);
            fields.add(new MutableObject<>(varExprRef));
            // add the same field as input to the corresponding local function propagating the type of the field
            expr = new AggregateFunctionCallExpression(typeFun, false,
                    Collections.singletonList(new MutableObject<>(varExprRef)));
            // add the type propagating function to the list of the local functions
            localOutVariable = context.newVar();
            localResultVariables.add(localOutVariable);
            localAggFunctions.add(new MutableObject<>(expr));
            // add the local result variable as input to the global range map function
            varExprRef = new VariableReferenceExpression(localOutVariable, sourceLocation);
            argsToRM.add(new MutableObject<>(varExprRef));
            ascendingFlags[i] = field.getOrder() == OrderOperator.IOrder.OrderKind.ASC;
            i++;
        }
        IFunctionInfo rangeMapFun = context.getMetadataProvider().lookupFunction(rangeMapFunction);
        AggregateFunctionCallExpression rangeMapExp = new AggregateFunctionCallExpression(rangeMapFun, true, argsToRM);
        rangeMapExp.setStepOneAggregate(samplingFun);
        rangeMapExp.setStepTwoAggregate(rangeMapFun);
        rangeMapExp.setSourceLocation(sourceLocation);
        rangeMapExp.setOpaqueParameters(new Object[] { numPartitions, ascendingFlags });
        globalResultVariable.add(context.newVar());
        globalAggFunction.add(new MutableObject<>(rangeMapExp));
    }

    /**
     * Creates an aggregate operator. $$resultVariables = expressions()
     * @param resultVariables the variables which stores the result of the aggregation
     * @param isGlobal whether the aggregate operator is a global or local one
     * @param expressions the aggregation functions desired
     * @param inputOperator the input op that is feeding the aggregate operator
     * @param context optimization context
     * @param sourceLocation source location
     * @return an aggregate operator with the specified information
     * @throws AlgebricksException when there is error setting the type environment of the newly created aggregate op
     */
    private static AggregateOperator createAggregate(List<LogicalVariable> resultVariables, boolean isGlobal,
            List<Mutable<ILogicalExpression>> expressions, MutableObject<ILogicalOperator> inputOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        AggregateOperator aggregateOperator = new AggregateOperator(resultVariables, expressions);
        aggregateOperator.setPhysicalOperator(new AggregatePOperator());
        aggregateOperator.setSourceLocation(sourceLocation);
        aggregateOperator.getInputs().add(inputOperator);
        aggregateOperator.setGlobal(isGlobal);
        if (!isGlobal) {
            aggregateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
        } else {
            aggregateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        }
        aggregateOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(aggregateOperator);
        return aggregateOperator;
    }

    private static ExchangeOperator createOneToOneExchangeOp(MutableObject<ILogicalOperator> inputOperator,
            IOptimizationContext context) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setPhysicalOperator(new OneToOneExchangePOperator());
        exchangeOperator.getInputs().add(inputOperator);
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static ForwardOperator createForward(String rangeMapKey, LogicalVariable rangeMapVariable,
            MutableObject<ILogicalOperator> exchangeOpFromReplicate, MutableObject<ILogicalOperator> globalAggInput,
            IOptimizationContext context, SourceLocation sourceLoc) throws AlgebricksException {
        AbstractLogicalExpression rangeMapExpression = new VariableReferenceExpression(rangeMapVariable, sourceLoc);
        ForwardOperator forwardOperator = new ForwardOperator(rangeMapKey, new MutableObject<>(rangeMapExpression));
        forwardOperator.setSourceLocation(sourceLoc);
        forwardOperator.setPhysicalOperator(new SortForwardPOperator());
        forwardOperator.getInputs().add(exchangeOpFromReplicate);
        forwardOperator.getInputs().add(globalAggInput);
        OperatorManipulationUtil.setOperatorMode(forwardOperator);
        forwardOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(forwardOperator);
        return forwardOperator;
    }

    private boolean allAreOrderProps(List<ILocalStructuralProperty> childLocalProperties) {
        for (ILocalStructuralProperty childLocalProperty : childLocalProperties) {
            if (childLocalProperty.getPropertyType() != PropertyType.LOCAL_ORDER_PROPERTY) {
                return false;
            }
        }
        return !childLocalProperties.isEmpty();
    }

    private void printOp(AbstractLogicalOperator op, IOptimizationContext ctx) throws AlgebricksException {
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            String plan = ctx.getPrettyPrinter().reset().printOperator(op).toString();
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(LogRedactionUtil.userData(plan));
        }
    }

    private List<OrderColumn> computeOrderColumns(IPhysicalPropertiesVector pv) {
        List<OrderColumn> ordCols = new ArrayList<>();
        List<ILocalStructuralProperty> localProps = pv.getLocalProperties();
        if (localProps == null || localProps.isEmpty()) {
            return new ArrayList<>();
        } else {
            for (ILocalStructuralProperty p : localProps) {
                if (p.getPropertyType() == PropertyType.LOCAL_ORDER_PROPERTY) {
                    LocalOrderProperty lop = (LocalOrderProperty) p;
                    ordCols.addAll(lop.getOrderColumns());
                } else {
                    return new ArrayList<>();
                }
            }
            return ordCols;
        }
    }

    private void setNewOp(Mutable<ILogicalOperator> opRef, AbstractLogicalOperator newOp, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator oldOp = opRef.getValue();
        opRef.setValue(newOp);
        newOp.getInputs().add(new MutableObject<>(oldOp));
        newOp.recomputeSchema();
        newOp.computeDeliveredPhysicalProperties(context);
        context.computeAndSetTypeEnvironmentForOperator(newOp);
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">>>> Structural properties for " + newOp.getPhysicalOperator()
                    + ": " + newOp.getDeliveredPhysicalProperties() + "\n");
        }

        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(newOp, context);
    }

}
