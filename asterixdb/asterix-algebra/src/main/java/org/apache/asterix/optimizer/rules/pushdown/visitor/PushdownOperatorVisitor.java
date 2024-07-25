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
package org.apache.asterix.optimizer.rules.pushdown.visitor;

import static org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams.DATABASE_NAME_POS;
import static org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams.DATASET_NAME_POS;
import static org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams.DATAVERSE_NAME_POS;
import static org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams.INDEX_NAME_POS;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.DatasetConfig.DatasetFormat;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SwitchOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * This visitor visits the entire plan and tries to build the information of the required values from all dataset
 */
public class PushdownOperatorVisitor implements ILogicalOperatorVisitor<Void, Void> {
    private static final List<LogicalVariable> EMPTY_VARIABLES = Collections.emptyList();
    private final PushdownContext pushdownContext;
    private final IOptimizationContext context;
    private final DefUseChainComputerVisitor defUseComputer;
    private final Set<ILogicalOperator> visitedOperators;

    public PushdownOperatorVisitor(PushdownContext pushdownContext, IOptimizationContext context) {
        this.pushdownContext = pushdownContext;
        this.context = context;
        defUseComputer = new DefUseChainComputerVisitor(pushdownContext);
        visitedOperators = new HashSet<>();
    }

    /**
     * Visit every input of an operator. Then, start pushdown any value expression that the operator has
     *
     * @param op                the operator to process
     * @param producedVariables any produced variables by the operator. We only care about the {@link AssignOperator}
     *                          and {@link UnnestOperator} variables for now.
     */
    private void visitInputs(ILogicalOperator op, List<LogicalVariable> producedVariables) throws AlgebricksException {
        if (visitedOperators.contains(op)) {
            return;
        }
        for (Mutable<ILogicalOperator> child : op.getInputs()) {
            child.getValue().accept(this, null);
        }
        visitedOperators.add(op);
        // Enter scope for (new stage) for operators like GROUP and JOIN
        pushdownContext.enterScope(op);
        defUseComputer.init(op, producedVariables);

        op.acceptExpressionTransform(defUseComputer);
        if (op.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
            // UnionAll is a special case
            UnionAllOperator unionOp = (UnionAllOperator) op;
            for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> vars : unionOp.getVariableMappings()) {
                VariableReferenceExpression left = new VariableReferenceExpression(vars.first);
                pushdownContext.use(op, left, -1, null);
                VariableReferenceExpression right = new VariableReferenceExpression(vars.second);
                pushdownContext.use(op, right, -1, null);
            }
        }
    }

    /*
     * ******************************************************************************
     * Operators that need to handle special cases
     * ******************************************************************************
     */

    @Override
    public Void visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        //Set as empty records for data-scan or unnest-map if certain variables are projected out
        setEmptyRecord(op.getInputs().get(0).getValue(), op.getVariables());
        return null;
    }

    /**
     * From the {@link DataSourceScanOperator}, we need to register the payload variable (record variable) to check
     * which expression in the plan is using it.
     */
    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        DatasetDataSource datasetDataSource = getDatasetDataSourceIfApplicable((DataSource) op.getDataSource());
        registerDatasetIfApplicable(datasetDataSource, op);
        visitInputs(op);
        return null;
    }

    /**
     * From the {@link UnnestMapOperator}, we need to register the payload variable (record variable) to check
     * which expression in the plan is using it.
     */
    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        DatasetDataSource datasetDataSource = getDatasetDataSourceIfApplicable(getDataSourceFromUnnestMapOperator(op));
        registerDatasetIfApplicable(datasetDataSource, op);
        return null;
    }

    /**
     * From the {@link LeftOuterUnnestMapOperator}, we need to register the payload variable (record variable) to check
     * which expression in the plan is using it.
     */
    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        DatasetDataSource datasetDataSource = getDatasetDataSourceIfApplicable(getDataSourceFromUnnestMapOperator(op));
        registerDatasetIfApplicable(datasetDataSource, op);
        return null;
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        visitInputs(op, op.getVariables());
        if (!op.isGlobal() && isCountConstant(op.getExpressions())) {
            /*
             * Optimize the SELECT COUNT(*) case
             * It is local aggregate and has agg-sql-count function with a constant argument. Set empty record if the
             * input operator is DataSourceScanOperator
             */
            setEmptyRecord(op.getInputs().get(0).getValue(), EMPTY_VARIABLES);
        }
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        visitInputs(op, op.getVariables());
        return null;
    }

    /*
     * ******************************************************************************
     * Helper methods
     * ******************************************************************************
     */

    /**
     * The role of this method is:
     * 1- Check whether the datasource allows value access pushdowns
     * 2- return the actual DatasetDataSource
     */
    private DatasetDataSource getDatasetDataSourceIfApplicable(DataSource dataSource) throws AlgebricksException {
        if (dataSource == null || dataSource.getDatasourceType() == DataSource.Type.SAMPLE
                || !(dataSource instanceof DatasetDataSource)) {
            return null;
        }

        Dataset dataset = getDataset(dataSource);
        //Only external dataset can have pushed down expressions
        if (dataset.getDatasetType() == DatasetConfig.DatasetType.INTERNAL
                && dataset.getDatasetFormatInfo().getFormat() == DatasetFormat.ROW) {
            return null;
        }

        return (DatasetDataSource) dataSource;
    }

    private Dataset getDataset(DataSource dataSource) throws AlgebricksException {
        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
        DataverseName dataverse = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasourceName();
        String database = dataSource.getId().getDatabaseName();
        return mp.findDataset(database, dataverse, datasetName);
    }

    /**
     * Find datasource from {@link UnnestMapOperator}
     *
     * @param unnest unnest map operator
     * @return datasource
     */
    private DataSource getDataSourceFromUnnestMapOperator(AbstractUnnestMapOperator unnest) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) unnest.getExpressionRef().getValue();
        String dataverse = ConstantExpressionUtil.getStringArgument(funcExpr, DATAVERSE_NAME_POS);
        String dataset = ConstantExpressionUtil.getStringArgument(funcExpr, DATASET_NAME_POS);
        if (!ConstantExpressionUtil.getStringArgument(funcExpr, INDEX_NAME_POS).equals(dataset)) {
            return null;
        }

        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverse);
        String database = ConstantExpressionUtil.getStringArgument(funcExpr, DATABASE_NAME_POS);
        DataSourceId dsid = new DataSourceId(database, dataverseName, dataset);
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        return metadataProvider.findDataSource(dsid);
    }

    private void registerDatasetIfApplicable(DatasetDataSource datasetDataSource, AbstractScanOperator op)
            throws AlgebricksException {
        if (datasetDataSource != null) {
            Dataset dataset = getDataset(datasetDataSource);
            List<LogicalVariable> primaryKeyVariables = datasetDataSource.getPrimaryKeyVariables(op.getVariables());
            LogicalVariable recordVar = datasetDataSource.getDataRecordVariable(op.getVariables());
            LogicalVariable metaVar = datasetDataSource.getMetaVariable(op.getVariables());
            pushdownContext.registerScan(dataset, primaryKeyVariables, recordVar, metaVar, op);
        }
    }

    /**
     * If the inputOp is a {@link DataSourceScanOperator} or {@link UnnestMapOperator}, then set the projected value
     * needed as empty record if any variable originated from either operators are not in {@code retainedVariables}
     *
     * @param inputOp           an operator that is potentially a {@link DataSourceScanOperator} or a {@link
     *                          UnnestMapOperator}
     * @param retainedVariables variables that should be retained
     * @see #visitAggregateOperator(AggregateOperator, Void)
     * @see #visitProjectOperator(ProjectOperator, Void)
     */
    private void setEmptyRecord(ILogicalOperator inputOp, List<LogicalVariable> retainedVariables)
            throws AlgebricksException {
        LogicalOperatorTag tag = inputOp.getOperatorTag();
        if (tag != LogicalOperatorTag.DATASOURCESCAN && tag != LogicalOperatorTag.UNNEST_MAP) {
            return;
        }

        DataSource dataSource;
        List<LogicalVariable> variables;
        Mutable<ILogicalExpression> selectCondition;
        if (inputOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator scan = (DataSourceScanOperator) inputOp;
            dataSource = (DataSource) scan.getDataSource();
            variables = scan.getVariables();
            selectCondition = scan.getSelectCondition();
        } else {
            UnnestMapOperator unnest = (UnnestMapOperator) inputOp;
            dataSource = getDataSourceFromUnnestMapOperator(unnest);
            variables = unnest.getVariables();
            selectCondition = unnest.getSelectCondition();
        }

        DatasetDataSource datasetDataSource = getDatasetDataSourceIfApplicable(dataSource);

        if (datasetDataSource == null) {
            //Does not support pushdown
            return;
        }

        Set<LogicalVariable> selectConditionVariables = new HashSet<>();
        if (selectCondition != null) {
            //Get the used variables for a select condition
            selectCondition.getValue().getUsedVariables(selectConditionVariables);
        }

        //We know that we only need the count of objects. So return empty objects only
        LogicalVariable recordVar = datasetDataSource.getDataRecordVariable(variables);
        ScanDefineDescriptor scanDefDesc = (ScanDefineDescriptor) pushdownContext.getDefineDescriptor(recordVar);

        /*
         * If the recordVar is not retained by an upper operator and not used by a select condition, then return empty
         * record instead of the entire record.
         */
        if (!retainedVariables.contains(recordVar) && !selectConditionVariables.contains(recordVar)) {
            /*
             * Set the root node as EMPTY_ROOT_NODE (i.e., no fields will be read from disk). We register the
             * dataset with EMPTY_ROOT_NODE so that we skip pushdowns on empty node.
             */
            scanDefDesc.setRecordNode(RootExpectedSchemaNode.EMPTY_ROOT_NODE);
        }

        if (scanDefDesc.hasMeta()) {
            //Do the same for meta
            if (!retainedVariables.contains(scanDefDesc.getMetaRecordVariable())) {
                scanDefDesc.setMetaNode(RootExpectedSchemaNode.EMPTY_ROOT_NODE);
            }
        }
    }

    private boolean isCountConstant(List<Mutable<ILogicalExpression>> expressions) {
        if (expressions.size() != 1) {
            return false;
        }
        ILogicalExpression expression = expressions.get(0).getValue();
        if (expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
        return BuiltinFunctions.SQL_COUNT.equals(fid)
                && funcExpr.getArguments().get(0).getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT;
    }

    private void visitNestedPlans(ILogicalOperator op, List<ILogicalPlan> nestedPlans) throws AlgebricksException {
        ILogicalOperator previousSubplanOp = pushdownContext.enterSubplan(op);
        for (ILogicalPlan plan : nestedPlans) {
            for (Mutable<ILogicalOperator> root : plan.getRoots()) {
                root.getValue().accept(this, null);
            }
        }
        pushdownContext.exitSubplan(previousSubplanOp);
    }

    /*
     * ******************************************************************************
     * Pushdown when possible for each operator
     * ******************************************************************************
     */

    @Override
    public Void visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        visitNestedPlans(op, op.getNestedPlans());
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        visitInputs(op, op.getVariables());
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
        visitInputs(op, op.getVariables());
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        visitInputs(op, op.getVariables());
        visitNestedPlans(op, op.getNestedPlans());
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitSwitchOperator(SwitchOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void arg)
            throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        visitNestedPlans(op, op.getNestedPlans());
        return null;
    }

    private void visitInputs(ILogicalOperator op) throws AlgebricksException {
        visitInputs(op, null);
    }
}
