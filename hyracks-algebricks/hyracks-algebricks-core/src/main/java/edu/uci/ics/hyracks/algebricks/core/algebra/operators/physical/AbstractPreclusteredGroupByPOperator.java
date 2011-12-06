package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LocalGroupingProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

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
        ILogicalOperator op2 = gby.getInputs().get(0).getOperator();
        IPhysicalPropertiesVector childProp = op2.getDeliveredPhysicalProperties();
        IPartitioningProperty pp = childProp.getPartitioningProperty();
        List<ILocalStructuralProperty> childLocals = childProp.getLocalProperties();
        if (childLocals != null) {
            for (ILocalStructuralProperty lsp : childLocals) {
                boolean failed = false;
                switch (lsp.getPropertyType()) {
                    case LOCAL_GROUPING_PROPERTY: {
                        LocalGroupingProperty lgp = (LocalGroupingProperty) lsp;
                        Set<LogicalVariable> colSet = new HashSet<LogicalVariable>();
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
                        OrderColumn oc = lop.getOrderColumn();
                        LogicalVariable v2 = getLhsGbyVar(gby, oc.getColumn());
                        if (v2 != null) {
                            propsLocal.add(new LocalOrderProperty(new OrderColumn(v2, oc.getOrder())));
                        } else {
                            failed = true;
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
        Set<LogicalVariable> gbvars = new HashSet<LogicalVariable>(columnList);
        LocalGroupingProperty groupProp = new LocalGroupingProperty(gbvars, new ArrayList<LogicalVariable>(columnList));

        GroupByOperator gby = (GroupByOperator) op;
        boolean goon = true;
        for (ILogicalPlan p : gby.getNestedPlans()) {
            // try to propagate secondary order requirements from nested
            // groupings
            for (LogicalOperatorReference r : p.getRoots()) {
                AbstractLogicalOperator op1 = (AbstractLogicalOperator) r.getOperator();
                if (op1.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                    AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getOperator();
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
                    LogicalVariable ord = lop.getColumn();
                    Pair<LogicalVariable, LogicalExpressionReference> p = getGbyPairByRhsVar(gby, ord);
                    if (p == null) {
                        p = getDecorPairByRhsVar(gby, ord);
                        if (p == null) {
                            allOk = false;
                            break;
                        }
                    }
                    ILogicalExpression e = p.second.getExpression();
                    if (e.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        throw new IllegalStateException(
                                "Right hand side of group-by assignment should have been normalized to a variable reference.");
                    }
                    LogicalVariable v = ((VariableReferenceExpression) e).getVariableReference();
                    props.add(new LocalOrderProperty(new OrderColumn(v, lop.getOrder())));
                }
                List<FunctionalDependency> fdList = new ArrayList<FunctionalDependency>();
                for (Pair<LogicalVariable, LogicalExpressionReference> decorPair : gby.getDecorList()) {
                    List<LogicalVariable> hd = gby.getGbyVarList();
                    List<LogicalVariable> tl = new ArrayList<LogicalVariable>(1);
                    tl.add(((VariableReferenceExpression) decorPair.second.getExpression()).getVariableReference());
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
            pp = new UnorderedPartitionedProperty(new HashSet<LogicalVariable>(columnList), null);
        }
        pv[0] = new StructuralPropertiesVector(pp, localProps);
        return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    private static Pair<LogicalVariable, LogicalExpressionReference> getGbyPairByRhsVar(GroupByOperator gby,
            LogicalVariable var) {
        for (Pair<LogicalVariable, LogicalExpressionReference> ve : gby.getGroupByList()) {
            if (ve.first == var) {
                return ve;
            }
        }
        return null;
    }

    private static Pair<LogicalVariable, LogicalExpressionReference> getDecorPairByRhsVar(GroupByOperator gby,
            LogicalVariable var) {
        for (Pair<LogicalVariable, LogicalExpressionReference> ve : gby.getDecorList()) {
            if (ve.first == var) {
                return ve;
            }
        }
        return null;
    }

    private static LogicalVariable getLhsGbyVar(GroupByOperator gby, LogicalVariable var) {
        for (Pair<LogicalVariable, LogicalExpressionReference> ve : gby.getGroupByList()) {
            ILogicalExpression e = ve.second.getExpression();
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

}
