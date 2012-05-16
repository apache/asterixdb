package edu.uci.ics.hyracks.algebricks.core.rewriter.base;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import edu.uci.ics.hyracks.algebricks.core.config.AlgebricksConfig;

public class HeuristicOptimizer {

    public static PhysicalOperatorTag[] hyraxOperators = new PhysicalOperatorTag[] {
            PhysicalOperatorTag.DATASOURCE_SCAN, PhysicalOperatorTag.BTREE_SEARCH,
            PhysicalOperatorTag.EXTERNAL_GROUP_BY, PhysicalOperatorTag.HASH_GROUP_BY, PhysicalOperatorTag.HDFS_READER,
            PhysicalOperatorTag.HYBRID_HASH_JOIN, PhysicalOperatorTag.IN_MEMORY_HASH_JOIN,
            PhysicalOperatorTag.NESTED_LOOP, PhysicalOperatorTag.PRE_SORTED_DISTINCT_BY,
            PhysicalOperatorTag.PRE_CLUSTERED_GROUP_BY, PhysicalOperatorTag.SPLIT, PhysicalOperatorTag.STABLE_SORT,
            PhysicalOperatorTag.UNION_ALL };
    public static PhysicalOperatorTag[] hyraxOperatorsBelowWhichJobGenIsDisabled = new PhysicalOperatorTag[] {};

    public static boolean isHyraxOp(PhysicalOperatorTag opTag) {
        for (PhysicalOperatorTag t : hyraxOperators) {
            if (t == opTag) {
                return true;
            }
        }
        return false;
    }

    private IOptimizationContext context;
    private List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites;
    private List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites;
    private ILogicalPlan plan;
    private LogicalOperatorPrettyPrintVisitor ppvisitor = new LogicalOperatorPrettyPrintVisitor();

    public HeuristicOptimizer(ILogicalPlan plan,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites,
            IOptimizationContext context) {
        this.plan = plan;
        this.context = context;
        this.logicalRewrites = logicalRewrites;
        this.physicalRewrites = physicalRewrites;
    }

    public void optimize() throws AlgebricksException {
        if (plan == null) {
            return;
        }
        if (AlgebricksConfig.DEBUG) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.fine("Starting logical optimizations.\n");
        }

        StringBuilder sb = new StringBuilder();
        PlanPrettyPrinter.printPlan(plan, sb, ppvisitor, 0);
        AlgebricksConfig.ALGEBRICKS_LOGGER.fine("Logical Plan:\n" + sb.toString());
        runOptimizationSets(plan, logicalRewrites);
        computeSchemaBottomUpForPlan(plan);
        runPhysicalOptimizations(plan, physicalRewrites);
        StringBuilder sb2 = new StringBuilder();
        PlanPrettyPrinter.printPlan(plan, sb2, ppvisitor, 0);
        AlgebricksConfig.ALGEBRICKS_LOGGER.fine("Optimized Plan:\n" + sb2.toString());
    }

    private void runOptimizationSets(ILogicalPlan plan,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> optimSet) throws AlgebricksException {
        for (Pair<AbstractRuleController, List<IAlgebraicRewriteRule>> ruleList : optimSet) {
            for (Mutable<ILogicalOperator> r : plan.getRoots()) {
                ruleList.first.setContext(context);
                ruleList.first.rewriteWithRuleCollection(r, ruleList.second);
            }
        }
    }

    private static void computeSchemaBottomUpForPlan(ILogicalPlan p) throws AlgebricksException {
        for (Mutable<ILogicalOperator> r : p.getRoots()) {
            computeSchemaBottomUpForOp((AbstractLogicalOperator) r.getValue());
        }
    }

    private static void computeSchemaBottomUpForOp(AbstractLogicalOperator op) throws AlgebricksException {
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            computeSchemaBottomUpForOp((AbstractLogicalOperator) i.getValue());
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans a = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : a.getNestedPlans()) {
                computeSchemaBottomUpForPlan(p);
            }
        }
        op.recomputeSchema();
    }

    private void runPhysicalOptimizations(ILogicalPlan plan,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites)
            throws AlgebricksException {
        if (AlgebricksConfig.DEBUG) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.fine("Starting physical optimizations.\n");
        }
        // PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(plan);
        runOptimizationSets(plan, physicalRewrites);
    }

}
