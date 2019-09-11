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
package org.apache.asterix.translator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslator;
import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.OrderbyClause.OrderModifier;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.IfExpr;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.QuantifiedExpression.Quantifier;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.util.RangeMapBuilder;
import org.apache.asterix.lang.common.visitor.base.AbstractQueryExpressionVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.LoadableDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.declared.ResultSetDataSink;
import org.apache.asterix.metadata.declared.ResultSetSinkId;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.functions.ExternalFunctionCompilerUtil;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.FunctionInfo;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.translator.CompiledStatements.CompiledInsertStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledLoadFromFileStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledUpsertStatement;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.asterix.translator.util.PlanTranslationUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation.BroadcastSide;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalOperatorDeepCopyWithNewVariablesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.result.IResultMetadata;

/**
 * Each visit returns a pair of an operator and a variable. The variable
 * corresponds to the new column, if any, added to the tuple flow. E.g., for
 * Unnest, the column is the variable bound to the elements in the list, for
 * Subplan it is null. The first argument of a visit method is the expression
 * which is translated. The second argument of a visit method is the tuple
 * source for the current subtree.
 */

class LangExpressionToPlanTranslator
        extends AbstractQueryExpressionVisitor<Pair<ILogicalOperator, LogicalVariable>, Mutable<ILogicalOperator>>
        implements ILangExpressionToPlanTranslator {

    protected final MetadataProvider metadataProvider;
    protected final TranslationContext context;
    private static final AtomicLong outputFileID = new AtomicLong(0);
    private static final String OUTPUT_FILE_PREFIX = "OUTPUT_";

    public LangExpressionToPlanTranslator(MetadataProvider metadataProvider, int currentVarCounterValue)
            throws AlgebricksException {
        this(metadataProvider, new Counter(currentVarCounterValue));
    }

    // Keeps the given Counter if one is provided instead of a value.
    public LangExpressionToPlanTranslator(MetadataProvider metadataProvider, Counter currentVarCounter)
            throws AlgebricksException {
        this.context = new TranslationContext(currentVarCounter);
        this.metadataProvider = metadataProvider;
    }

    @Override
    public int getVarCounter() {
        return context.getVarCounter();
    }

    @Override
    public ILogicalPlan translateLoad(ICompiledDmlStatement stmt) throws AlgebricksException {
        CompiledLoadFromFileStatement clffs = (CompiledLoadFromFileStatement) stmt;
        SourceLocation sourceLoc = stmt.getSourceLocation();
        Dataset dataset = metadataProvider.findDataset(clffs.getDataverseName(), clffs.getDatasetName());
        if (dataset == null) {
            // This would never happen since we check for this in AqlTranslator
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, clffs.getDatasetName(),
                    clffs.getDataverseName());
        }
        IAType itemType = metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        IAType metaItemType =
                metadataProvider.findType(dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());
        DatasetDataSource targetDatasource =
                validateDatasetInfo(metadataProvider, stmt.getDataverseName(), stmt.getDatasetName(), sourceLoc);
        List<List<String>> partitionKeys = targetDatasource.getDataset().getPrimaryKeys();
        if (dataset.hasMetaPart()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    dataset.getDatasetName() + ": load dataset is not supported on Datasets with Meta records");
        }

        LoadableDataSource lds;
        try {
            lds = new LoadableDataSource(dataset, itemType, metaItemType, clffs.getAdapter(), clffs.getProperties());
        } catch (IOException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, e.toString(), e);
        }

        // etsOp is a dummy input operator used to keep the compiler happy. it
        // could be removed but would result in
        // the need to fix many rewrite rules that assume that datasourcescan
        // operators always have input.
        ILogicalOperator etsOp = new EmptyTupleSourceOperator();

        // Add a logical variable for the record.
        List<LogicalVariable> payloadVars = new ArrayList<>();
        payloadVars.add(context.newVar());

        // Create a scan operator and make the empty tuple source its input
        DataSourceScanOperator dssOp = new DataSourceScanOperator(payloadVars, lds);
        dssOp.getInputs().add(new MutableObject<>(etsOp));
        dssOp.setSourceLocation(sourceLoc);

        VariableReferenceExpression payloadExpr = new VariableReferenceExpression(payloadVars.get(0));
        payloadExpr.setSourceLocation(sourceLoc);

        Mutable<ILogicalExpression> payloadRef = new MutableObject<>(payloadExpr);

        // Creating the assign to extract the PK out of the record
        ArrayList<LogicalVariable> pkVars = new ArrayList<>();
        ArrayList<Mutable<ILogicalExpression>> pkExprs = new ArrayList<>();
        List<Mutable<ILogicalExpression>> varRefsForLoading = new ArrayList<>();
        LogicalVariable payloadVar = payloadVars.get(0);
        for (List<String> keyFieldName : partitionKeys) {
            PlanTranslationUtil.prepareVarAndExpression(keyFieldName, payloadVar, pkVars, pkExprs, varRefsForLoading,
                    context, sourceLoc);
        }

        AssignOperator assign = new AssignOperator(pkVars, pkExprs);
        assign.getInputs().add(new MutableObject<>(dssOp));
        assign.setSourceLocation(sourceLoc);

        // If the input is pre-sorted, we set the ordering property explicitly in the
        // assign
        if (clffs.alreadySorted()) {
            List<OrderColumn> orderColumns = new ArrayList<>();
            for (int i = 0; i < pkVars.size(); ++i) {
                orderColumns.add(new OrderColumn(pkVars.get(i), OrderKind.ASC));
            }
            assign.setExplicitOrderingProperty(new LocalOrderProperty(orderColumns));
        }

        List<String> additionalFilteringField = DatasetUtil.getFilterField(targetDatasource.getDataset());
        List<LogicalVariable> additionalFilteringVars;
        List<Mutable<ILogicalExpression>> additionalFilteringAssignExpressions;
        List<Mutable<ILogicalExpression>> additionalFilteringExpressions = null;
        AssignOperator additionalFilteringAssign = null;
        if (additionalFilteringField != null) {
            additionalFilteringVars = new ArrayList<>();
            additionalFilteringAssignExpressions = new ArrayList<>();
            additionalFilteringExpressions = new ArrayList<>();
            PlanTranslationUtil.prepareVarAndExpression(additionalFilteringField, payloadVar, additionalFilteringVars,
                    additionalFilteringAssignExpressions, additionalFilteringExpressions, context, sourceLoc);
            additionalFilteringAssign =
                    new AssignOperator(additionalFilteringVars, additionalFilteringAssignExpressions);
            additionalFilteringAssign.setSourceLocation(sourceLoc);
        }

        InsertDeleteUpsertOperator insertOp = new InsertDeleteUpsertOperator(targetDatasource, payloadRef,
                varRefsForLoading, InsertDeleteUpsertOperator.Kind.INSERT, true);
        insertOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
        insertOp.setSourceLocation(sourceLoc);

        if (additionalFilteringAssign != null) {
            additionalFilteringAssign.getInputs().add(new MutableObject<>(assign));
            insertOp.getInputs().add(new MutableObject<>(additionalFilteringAssign));
        } else {
            insertOp.getInputs().add(new MutableObject<>(assign));
        }

        SinkOperator leafOperator = new SinkOperator();
        leafOperator.getInputs().add(new MutableObject<>(insertOp));
        leafOperator.setSourceLocation(sourceLoc);
        return new ALogicalPlanImpl(new MutableObject<>(leafOperator));
    }

    @Override
    public ILogicalPlan translate(Query expr, String outputDatasetName, ICompiledDmlStatement stmt,
            IResultMetadata resultMetadata) throws AlgebricksException {
        return translate(expr, outputDatasetName, stmt, null, resultMetadata);
    }

    public ILogicalPlan translate(Query expr, String outputDatasetName, ICompiledDmlStatement stmt,
            ILogicalOperator baseOp, IResultMetadata resultMetadata) throws AlgebricksException {
        MutableObject<ILogicalOperator> base = new MutableObject<>(new EmptyTupleSourceOperator());
        if (baseOp != null) {
            base = new MutableObject<>(baseOp);
        }
        SourceLocation sourceLoc = expr.getSourceLocation();
        Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this, base);
        ArrayList<Mutable<ILogicalOperator>> globalPlanRoots = new ArrayList<>();
        ILogicalOperator topOp = p.first;
        List<LogicalVariable> liveVars = new ArrayList<>();
        VariableUtilities.getLiveVariables(topOp, liveVars);
        LogicalVariable unnestVar = liveVars.get(0);
        LogicalVariable resVar = unnestVar;

        if (outputDatasetName == null) {
            FileSplit outputFileSplit = metadataProvider.getOutputFile();
            if (outputFileSplit == null) {
                outputFileSplit = getDefaultOutputFileLocation(metadataProvider.getApplicationContext());
            }
            metadataProvider.setOutputFile(outputFileSplit);

            List<Mutable<ILogicalExpression>> writeExprList = new ArrayList<>(1);
            VariableReferenceExpression resVarRef = new VariableReferenceExpression(resVar);
            resVarRef.setSourceLocation(sourceLoc);
            writeExprList.add(new MutableObject<>(resVarRef));
            ResultSetSinkId rssId = new ResultSetSinkId(metadataProvider.getResultSetId());
            ResultSetDataSink sink = new ResultSetDataSink(rssId, null);
            DistributeResultOperator newTop = new DistributeResultOperator(writeExprList, sink, resultMetadata);
            newTop.setSourceLocation(sourceLoc);
            newTop.getInputs().add(new MutableObject<>(topOp));
            topOp = newTop;

            // Retrieve the Output RecordType (if any) and store it on
            // the DistributeResultOperator
            IAType outputRecordType = metadataProvider.findOutputRecordType();
            if (outputRecordType != null) {
                topOp.getAnnotations().put("output-record-type", outputRecordType);
            }
        } else {
            /**
             * add the collection-to-sequence right before the project, because dataset only
             * accept non-collection records
             */
            LogicalVariable seqVar = context.newVar();
            /**
             * This assign adds a marker function collection-to-sequence: if the input is a
             * singleton collection, unnest it; otherwise do nothing.
             */
            VariableReferenceExpression resVarRef = new VariableReferenceExpression(resVar);
            resVarRef.setSourceLocation(sourceLoc);
            ScalarFunctionCallExpression collectionToSequenceExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.COLLECTION_TO_SEQUENCE),
                    new MutableObject<>(resVarRef));
            collectionToSequenceExpr.setSourceLocation(sourceLoc);
            AssignOperator assignCollectionToSequence =
                    new AssignOperator(seqVar, new MutableObject<>(collectionToSequenceExpr));
            assignCollectionToSequence.setSourceLocation(sourceLoc);

            assignCollectionToSequence.getInputs().add(new MutableObject<>(topOp.getInputs().get(0).getValue()));
            topOp.getInputs().get(0).setValue(assignCollectionToSequence);
            ProjectOperator projectOperator = (ProjectOperator) topOp;
            projectOperator.getVariables().set(0, seqVar);
            resVar = seqVar;
            DatasetDataSource targetDatasource =
                    validateDatasetInfo(metadataProvider, stmt.getDataverseName(), stmt.getDatasetName(), sourceLoc);
            List<Integer> keySourceIndicator =
                    ((InternalDatasetDetails) targetDatasource.getDataset().getDatasetDetails())
                            .getKeySourceIndicator();
            ArrayList<LogicalVariable> vars = new ArrayList<>();
            ArrayList<Mutable<ILogicalExpression>> exprs = new ArrayList<>();
            List<Mutable<ILogicalExpression>> varRefsForLoading = new ArrayList<>();
            List<List<String>> partitionKeys = targetDatasource.getDataset().getPrimaryKeys();
            int numOfPrimaryKeys = partitionKeys.size();
            for (int i = 0; i < numOfPrimaryKeys; i++) {
                if (keySourceIndicator == null || keySourceIndicator.get(i).intValue() == 0) {
                    // record part
                    PlanTranslationUtil.prepareVarAndExpression(partitionKeys.get(i), resVar, vars, exprs,
                            varRefsForLoading, context, sourceLoc);
                } else {
                    // meta part
                    PlanTranslationUtil.prepareMetaKeyAccessExpression(partitionKeys.get(i), unnestVar, exprs, vars,
                            varRefsForLoading, context, sourceLoc);
                }
            }

            AssignOperator assign = new AssignOperator(vars, exprs);
            assign.setSourceLocation(sourceLoc);
            List<String> additionalFilteringField = DatasetUtil.getFilterField(targetDatasource.getDataset());
            List<LogicalVariable> additionalFilteringVars;
            List<Mutable<ILogicalExpression>> additionalFilteringAssignExpressions;
            List<Mutable<ILogicalExpression>> additionalFilteringExpressions = null;
            AssignOperator additionalFilteringAssign = null;
            if (additionalFilteringField != null) {
                additionalFilteringVars = new ArrayList<>();
                additionalFilteringAssignExpressions = new ArrayList<>();
                additionalFilteringExpressions = new ArrayList<>();

                PlanTranslationUtil.prepareVarAndExpression(additionalFilteringField, resVar, additionalFilteringVars,
                        additionalFilteringAssignExpressions, additionalFilteringExpressions, context, sourceLoc);

                additionalFilteringAssign =
                        new AssignOperator(additionalFilteringVars, additionalFilteringAssignExpressions);
                additionalFilteringAssign.getInputs().add(new MutableObject<>(topOp));
                additionalFilteringAssign.setSourceLocation(sourceLoc);
                assign.getInputs().add(new MutableObject<>(additionalFilteringAssign));
            } else {
                assign.getInputs().add(new MutableObject<>(topOp));
            }

            VariableReferenceExpression resVarRef2 = new VariableReferenceExpression(resVar);
            resVarRef2.setSourceLocation(sourceLoc);
            Mutable<ILogicalExpression> varRef = new MutableObject<>(resVarRef2);
            ILogicalOperator leafOperator;
            switch (stmt.getKind()) {
                case INSERT:
                    leafOperator = translateInsert(targetDatasource, varRef, varRefsForLoading,
                            additionalFilteringExpressions, assign, stmt, resultMetadata);
                    break;
                case UPSERT:
                    leafOperator = translateUpsert(targetDatasource, varRef, varRefsForLoading,
                            additionalFilteringExpressions, assign, additionalFilteringField, unnestVar, topOp, exprs,
                            resVar, additionalFilteringAssign, stmt, resultMetadata);
                    break;
                case DELETE:
                    leafOperator = translateDelete(targetDatasource, varRef, varRefsForLoading,
                            additionalFilteringExpressions, assign, stmt);
                    break;
                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Unsupported statement kind " + stmt.getKind());
            }
            topOp = leafOperator;
        }
        globalPlanRoots.add(new MutableObject<>(topOp));
        ILogicalPlan plan = new ALogicalPlanImpl(globalPlanRoots);
        eliminateSharedOperatorReferenceForPlan(plan);
        return plan;
    }

    private ILogicalOperator translateDelete(DatasetDataSource targetDatasource, Mutable<ILogicalExpression> varRef,
            List<Mutable<ILogicalExpression>> varRefsForLoading,
            List<Mutable<ILogicalExpression>> additionalFilteringExpressions, ILogicalOperator assign,
            ICompiledDmlStatement stmt) throws AlgebricksException {
        SourceLocation sourceLoc = stmt.getSourceLocation();
        if (targetDatasource.getDataset().hasMetaPart()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    targetDatasource.getDataset().getDatasetName()
                            + ": delete from dataset is not supported on Datasets with Meta records");
        }
        InsertDeleteUpsertOperator deleteOp = new InsertDeleteUpsertOperator(targetDatasource, varRef,
                varRefsForLoading, InsertDeleteUpsertOperator.Kind.DELETE, false);
        deleteOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
        deleteOp.getInputs().add(new MutableObject<>(assign));
        deleteOp.setSourceLocation(sourceLoc);
        DelegateOperator leafOperator = new DelegateOperator(new CommitOperator(true));
        leafOperator.getInputs().add(new MutableObject<>(deleteOp));
        leafOperator.setSourceLocation(sourceLoc);
        return leafOperator;
    }

    private ILogicalOperator translateUpsert(DatasetDataSource targetDatasource, Mutable<ILogicalExpression> varRef,
            List<Mutable<ILogicalExpression>> varRefsForLoading,
            List<Mutable<ILogicalExpression>> additionalFilteringExpressions, ILogicalOperator assign,
            List<String> additionalFilteringField, LogicalVariable unnestVar, ILogicalOperator topOp,
            List<Mutable<ILogicalExpression>> exprs, LogicalVariable resVar, AssignOperator additionalFilteringAssign,
            ICompiledDmlStatement stmt, IResultMetadata resultMetadata) throws AlgebricksException {
        SourceLocation sourceLoc = stmt.getSourceLocation();
        if (!targetDatasource.getDataset().allow(topOp, DatasetUtil.OP_UPSERT)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    targetDatasource.getDataset().getDatasetName()
                            + ": upsert into dataset is not supported on Datasets with Meta records");
        }
        ProjectOperator project = (ProjectOperator) topOp;
        CompiledUpsertStatement compiledUpsert = (CompiledUpsertStatement) stmt;
        Expression returnExpression = compiledUpsert.getReturnExpression();
        InsertDeleteUpsertOperator upsertOp;
        ILogicalOperator rootOperator;
        if (targetDatasource.getDataset().hasMetaPart()) {
            if (returnExpression != null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Returning not allowed on datasets with Meta records");
            }
            AssignOperator metaAndKeysAssign;
            List<LogicalVariable> metaAndKeysVars;
            List<Mutable<ILogicalExpression>> metaAndKeysExprs;
            List<Mutable<ILogicalExpression>> metaExpSingletonList;
            metaAndKeysVars = new ArrayList<>();
            metaAndKeysExprs = new ArrayList<>();
            // add the meta function
            IFunctionInfo finfoMeta = FunctionUtil.getFunctionInfo(BuiltinFunctions.META);
            VariableReferenceExpression unnestVarRef = new VariableReferenceExpression(unnestVar);
            unnestVarRef.setSourceLocation(sourceLoc);
            ScalarFunctionCallExpression metaFunction =
                    new ScalarFunctionCallExpression(finfoMeta, new MutableObject<>(unnestVarRef));
            metaFunction.setSourceLocation(sourceLoc);
            // create assign for the meta part
            LogicalVariable metaVar = context.newVar();
            metaExpSingletonList = new ArrayList<>(1);
            VariableReferenceExpression metaVarRef = new VariableReferenceExpression(metaVar);
            metaVarRef.setSourceLocation(sourceLoc);
            metaExpSingletonList.add(new MutableObject<>(metaVarRef));
            metaAndKeysVars.add(metaVar);
            metaAndKeysExprs.add(new MutableObject<>(metaFunction));
            project.getVariables().add(metaVar);
            varRefsForLoading.clear();
            for (Mutable<ILogicalExpression> assignExpr : exprs) {
                if (assignExpr.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression funcCall = (AbstractFunctionCallExpression) assignExpr.getValue();
                    funcCall.substituteVar(resVar, unnestVar);
                    LogicalVariable pkVar = context.newVar();
                    metaAndKeysVars.add(pkVar);
                    metaAndKeysExprs.add(new MutableObject<>(assignExpr.getValue()));
                    project.getVariables().add(pkVar);
                    varRefsForLoading.add(new MutableObject<>(new VariableReferenceExpression(pkVar)));
                }
            }
            // A change feed, we don't need the assign to access PKs
            upsertOp = new InsertDeleteUpsertOperator(targetDatasource, varRef, varRefsForLoading, metaExpSingletonList,
                    InsertDeleteUpsertOperator.Kind.UPSERT, false);
            upsertOp.setUpsertIndicatorVar(context.newVar());
            upsertOp.setUpsertIndicatorVarType(BuiltinType.ABOOLEAN);
            // Create and add a new variable used for representing the original record
            upsertOp.setPrevRecordVar(context.newVar());
            upsertOp.setPrevRecordType(targetDatasource.getItemType());
            upsertOp.setSourceLocation(sourceLoc);
            if (targetDatasource.getDataset().hasMetaPart()) {
                List<LogicalVariable> metaVars = new ArrayList<>();
                metaVars.add(context.newVar());
                upsertOp.setPrevAdditionalNonFilteringVars(metaVars);
                List<Object> metaTypes = new ArrayList<>();
                metaTypes.add(targetDatasource.getMetaItemType());
                upsertOp.setPrevAdditionalNonFilteringTypes(metaTypes);
            }

            if (additionalFilteringField != null) {
                upsertOp.setPrevFilterVar(context.newVar());
                upsertOp.setPrevFilterType(
                        ((ARecordType) targetDatasource.getItemType()).getFieldType(additionalFilteringField.get(0)));
                additionalFilteringAssign.getInputs().clear();
                additionalFilteringAssign.getInputs().add(assign.getInputs().get(0));
                upsertOp.getInputs().add(new MutableObject<>(additionalFilteringAssign));
            } else {
                upsertOp.getInputs().add(assign.getInputs().get(0));
            }
            metaAndKeysAssign = new AssignOperator(metaAndKeysVars, metaAndKeysExprs);
            metaAndKeysAssign.getInputs().add(topOp.getInputs().get(0));
            metaAndKeysAssign.setSourceLocation(sourceLoc);
            topOp.getInputs().set(0, new MutableObject<>(metaAndKeysAssign));
            upsertOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
        } else {
            upsertOp = new InsertDeleteUpsertOperator(targetDatasource, varRef, varRefsForLoading,
                    InsertDeleteUpsertOperator.Kind.UPSERT, false);
            upsertOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
            upsertOp.getInputs().add(new MutableObject<>(assign));
            upsertOp.setSourceLocation(sourceLoc);
            upsertOp.setUpsertIndicatorVar(context.newVar());
            upsertOp.setUpsertIndicatorVarType(BuiltinType.ABOOLEAN);
            // Create and add a new variable used for representing the original record
            ARecordType recordType = (ARecordType) targetDatasource.getItemType();
            upsertOp.setPrevRecordVar(context.newVar());
            upsertOp.setPrevRecordType(recordType);
            if (additionalFilteringField != null) {
                upsertOp.setPrevFilterVar(context.newVar());
                upsertOp.setPrevFilterType(recordType.getFieldType(additionalFilteringField.get(0)));
            }
        }
        DelegateOperator delegateOperator = new DelegateOperator(new CommitOperator(returnExpression == null));
        delegateOperator.getInputs().add(new MutableObject<>(upsertOp));
        delegateOperator.setSourceLocation(sourceLoc);
        rootOperator = delegateOperator;

        // Compiles the return expression.
        return processReturningExpression(rootOperator, upsertOp, compiledUpsert, resultMetadata);
    }

    private ILogicalOperator translateInsert(DatasetDataSource targetDatasource, Mutable<ILogicalExpression> varRef,
            List<Mutable<ILogicalExpression>> varRefsForLoading,
            List<Mutable<ILogicalExpression>> additionalFilteringExpressions, ILogicalOperator assign,
            ICompiledDmlStatement stmt, IResultMetadata resultMetadata) throws AlgebricksException {
        SourceLocation sourceLoc = stmt.getSourceLocation();
        if (targetDatasource.getDataset().hasMetaPart()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    targetDatasource.getDataset().getDatasetName()
                            + ": insert into dataset is not supported on Datasets with Meta records");
        }
        // Adds the insert operator.
        InsertDeleteUpsertOperator insertOp = new InsertDeleteUpsertOperator(targetDatasource, varRef,
                varRefsForLoading, InsertDeleteUpsertOperator.Kind.INSERT, false);
        insertOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
        insertOp.getInputs().add(new MutableObject<>(assign));
        insertOp.setSourceLocation(sourceLoc);

        // Adds the commit operator.
        CompiledInsertStatement compiledInsert = (CompiledInsertStatement) stmt;
        Expression returnExpression = compiledInsert.getReturnExpression();
        DelegateOperator rootOperator = new DelegateOperator(new CommitOperator(returnExpression == null));
        rootOperator.getInputs().add(new MutableObject<>(insertOp));
        rootOperator.setSourceLocation(sourceLoc);

        // Compiles the return expression.
        return processReturningExpression(rootOperator, insertOp, compiledInsert, resultMetadata);
    }

    // Stitches the translated operators for the returning expression into the query
    // plan.
    private ILogicalOperator processReturningExpression(ILogicalOperator inputOperator,
            InsertDeleteUpsertOperator insertOp, CompiledInsertStatement compiledInsert, IResultMetadata resultMetadata)
            throws AlgebricksException {
        Expression returnExpression = compiledInsert.getReturnExpression();
        if (returnExpression == null) {
            return inputOperator;
        }
        SourceLocation sourceLoc = compiledInsert.getSourceLocation();

        //Create an assign operator that makes the variable used by the return expression
        LogicalVariable insertedVar = context.newVar();
        AssignOperator insertedVarAssignOperator =
                new AssignOperator(insertedVar, new MutableObject<>(insertOp.getPayloadExpression().getValue()));
        insertedVarAssignOperator.getInputs().add(insertOp.getInputs().get(0));
        insertedVarAssignOperator.setSourceLocation(sourceLoc);
        insertOp.getInputs().set(0, new MutableObject<>(insertedVarAssignOperator));

        // Makes the id of the insert var point to the record variable.
        context.newVarFromExpression(compiledInsert.getVar());
        context.setVar(compiledInsert.getVar(), insertedVar);

        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p =
                langExprToAlgExpression(returnExpression, new MutableObject<>(inputOperator));

        // Adds an assign operator for the result of the returning expression.
        LogicalVariable resultVar = context.newVar();
        AssignOperator createResultAssignOperator = new AssignOperator(resultVar, new MutableObject<>(p.first));
        createResultAssignOperator.getInputs().add(p.second);
        createResultAssignOperator.setSourceLocation(sourceLoc);

        // Adds a distribute result operator.
        List<Mutable<ILogicalExpression>> expressions = new ArrayList<>();
        expressions.add(new MutableObject<>(new VariableReferenceExpression(resultVar)));
        ResultSetSinkId rssId = new ResultSetSinkId(metadataProvider.getResultSetId());
        ResultSetDataSink sink = new ResultSetDataSink(rssId, null);
        DistributeResultOperator distResultOperator = new DistributeResultOperator(expressions, sink, resultMetadata);
        distResultOperator.getInputs().add(new MutableObject<>(createResultAssignOperator));

        distResultOperator.setSourceLocation(sourceLoc);
        return distResultOperator;
    }

    private DatasetDataSource validateDatasetInfo(MetadataProvider metadataProvider, String dataverseName,
            String datasetName, SourceLocation sourceLoc) throws AlgebricksException {
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                    dataverseName);
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Cannot write output to an external dataset.");
        }
        DataSourceId sourceId = new DataSourceId(dataverseName, datasetName);
        IAType itemType = metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        IAType metaItemType =
                metadataProvider.findType(dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());
        INodeDomain domain = metadataProvider.findNodeDomain(dataset.getNodeGroupName());
        return new DatasetDataSource(sourceId, dataset, itemType, metaItemType, DataSource.Type.INTERNAL_DATASET,
                dataset.getDatasetDetails(), domain);
    }

    private FileSplit getDefaultOutputFileLocation(ICcApplicationContext appCtx) throws AlgebricksException {
        String outputDir = System.getProperty("java.io.tmpDir");
        String filePath =
                outputDir + System.getProperty("file.separator") + OUTPUT_FILE_PREFIX + outputFileID.incrementAndGet();
        MetadataProperties metadataProperties = appCtx.getMetadataProperties();
        return new ManagedFileSplit(metadataProperties.getMetadataNodeName(), filePath);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(LetClause lc, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        LogicalVariable v;
        AssignOperator returnedOp;
        Expression bindingExpr = lc.getBindingExpr();
        SourceLocation sourceLoc = bindingExpr.getSourceLocation();
        if (bindingExpr.getKind() == Kind.VARIABLE_EXPRESSION) {
            VariableExpr bindingVarExpr = (VariableExpr) bindingExpr;
            ILogicalExpression prevVarRef = translateVariableRef(bindingVarExpr);
            v = context.newVarFromExpression(lc.getVarExpr());
            returnedOp = new AssignOperator(v, new MutableObject<>(prevVarRef));
            returnedOp.getInputs().add(tupSource);
            returnedOp.setSourceLocation(sourceLoc);
        } else {
            v = context.newVarFromExpression(lc.getVarExpr());
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(bindingExpr, tupSource);
            returnedOp = new AssignOperator(v, new MutableObject<>(eo.first));
            returnedOp.getInputs().add(eo.second);
            returnedOp.setSourceLocation(sourceLoc);
        }

        return new Pair<>(returnedOp, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FieldAccessor fa, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        SourceLocation sourceLoc = fa.getSourceLocation();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(fa.getExpr(), tupSource);
        LogicalVariable v = context.newVarFromExpression(fa);
        AbstractFunctionCallExpression fldAccess =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME));
        fldAccess.setSourceLocation(sourceLoc);
        fldAccess.getArguments().add(new MutableObject<>(p.first));
        ConstantExpression faExpr =
                new ConstantExpression(new AsterixConstantValue(new AString(fa.getIdent().getValue())));
        faExpr.setSourceLocation(sourceLoc);
        fldAccess.getArguments().add(new MutableObject<>(faExpr));
        AssignOperator a = new AssignOperator(v, new MutableObject<>(fldAccess));
        a.getInputs().add(p.second);
        a.setSourceLocation(sourceLoc);
        return new Pair<>(a, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(IndexAccessor ia, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        SourceLocation sourceLoc = ia.getSourceLocation();

        // Expression pair
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> expressionPair =
                langExprToAlgExpression(ia.getExpr(), tupSource);
        LogicalVariable v = context.newVar();
        AbstractFunctionCallExpression f;

        // Index expression
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> indexPair = null;

        if (ia.isAny()) {
            f = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.ANY_COLLECTION_MEMBER));
            f.getArguments().add(new MutableObject<>(expressionPair.first));
        } else {
            indexPair = langExprToAlgExpression(ia.getIndexExpr(), expressionPair.second);
            f = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.GET_ITEM));
            f.getArguments().add(new MutableObject<>(expressionPair.first));
            f.getArguments().add(new MutableObject<>(indexPair.first));
        }

        f.setSourceLocation(sourceLoc);
        AssignOperator a = new AssignOperator(v, new MutableObject<>(f));

        if (ia.isAny()) {
            a.getInputs().add(expressionPair.second);
        } else {
            a.getInputs().add(indexPair.second); // NOSONAR: Called only if value exists
        }
        a.setSourceLocation(sourceLoc);
        return new Pair<>(a, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(ListSliceExpression expression,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        SourceLocation sourceLoc = expression.getSourceLocation();

        // Expression pair
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> expressionPair =
                langExprToAlgExpression(expression.getExpr(), tupSource);
        LogicalVariable variable = context.newVar();
        AbstractFunctionCallExpression functionCallExpression;

        // Start index expression pair
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> startIndexPair =
                langExprToAlgExpression(expression.getStartIndexExpression(), expressionPair.second);

        // End index expression can be null (optional)
        // End index expression pair
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> endIndexPair = null;
        if (expression.hasEndExpression()) {
            endIndexPair = langExprToAlgExpression(expression.getEndIndexExpression(), startIndexPair.second);
            functionCallExpression = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.ARRAY_SLICE_WITH_END_POSITION));
            functionCallExpression.getArguments().add(new MutableObject<>(expressionPair.first));
            functionCallExpression.getArguments().add(new MutableObject<>(startIndexPair.first));
            functionCallExpression.getArguments().add(new MutableObject<>(endIndexPair.first));
            functionCallExpression.setSourceLocation(sourceLoc);
        } else {
            functionCallExpression = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.ARRAY_SLICE_WITHOUT_END_POSITION));
            functionCallExpression.getArguments().add(new MutableObject<>(expressionPair.first));
            functionCallExpression.getArguments().add(new MutableObject<>(startIndexPair.first));
            functionCallExpression.setSourceLocation(sourceLoc);
        }

        AssignOperator assignOperator = new AssignOperator(variable, new MutableObject<>(functionCallExpression));

        if (expression.hasEndExpression()) {
            assignOperator.getInputs().add(endIndexPair.second); // NOSONAR: Called only if value exists
        } else {
            assignOperator.getInputs().add(startIndexPair.second);
        }

        assignOperator.setSourceLocation(sourceLoc);
        return new Pair<>(assignOperator, variable);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CallExpr fcall, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        LogicalVariable v = context.newVar();
        FunctionSignature signature = fcall.getFunctionSignature();
        List<Mutable<ILogicalExpression>> args = new ArrayList<>();
        Mutable<ILogicalOperator> topOp = tupSource;

        for (Expression expr : fcall.getExprList()) {
            switch (expr.getKind()) {
                case VARIABLE_EXPRESSION:
                    VariableExpr varExpr = (VariableExpr) expr;
                    ILogicalExpression varRefExpr = translateVariableRef(varExpr);
                    args.add(new MutableObject<>(varRefExpr));
                    break;
                case LITERAL_EXPRESSION:
                    LiteralExpr val = (LiteralExpr) expr;
                    AsterixConstantValue cValue =
                            new AsterixConstantValue(ConstantHelper.objectFromLiteral(val.getValue()));
                    ConstantExpression cExpr = new ConstantExpression(cValue);
                    cExpr.setSourceLocation(expr.getSourceLocation());
                    args.add(new MutableObject<>(cExpr));
                    break;
                default:
                    Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(expr, topOp);
                    AbstractLogicalOperator o1 = (AbstractLogicalOperator) eo.second.getValue();
                    args.add(new MutableObject<>(eo.first));
                    if (o1 != null) {
                        topOp = eo.second;
                    }
                    break;
            }
        }

        SourceLocation sourceLoc = fcall.getSourceLocation();

        AbstractFunctionCallExpression f = lookupFunction(signature, args, sourceLoc);

        if (f == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, sourceLoc, signature.getName());
        }

        // Put hints into function call expr.
        if (fcall.hasHints()) {
            for (IExpressionAnnotation hint : fcall.getHints()) {
                f.getAnnotations().put(hint, hint);
            }
        }

        AssignOperator op = new AssignOperator(v, new MutableObject<>(f));
        if (topOp != null) {
            op.getInputs().add(topOp);
        }
        op.setSourceLocation(sourceLoc);

        return new Pair<>(op, v);
    }

    protected ILogicalExpression translateVariableRef(VariableExpr varExpr) throws CompilationException {
        LogicalVariable var = context.getVar(varExpr.getVar().getId());
        if (var == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, varExpr.getSourceLocation(),
                    varExpr.toString());
        }
        VariableReferenceExpression varRef = new VariableReferenceExpression(var);
        varRef.setSourceLocation(varExpr.getSourceLocation());
        return varRef;
    }

    protected AbstractFunctionCallExpression lookupFunction(FunctionSignature signature,
            List<Mutable<ILogicalExpression>> args, SourceLocation sourceLoc) throws CompilationException {
        AbstractFunctionCallExpression f;
        if ((f = lookupUserDefinedFunction(signature, args, sourceLoc)) == null) {
            f = lookupBuiltinFunction(signature.getName(), signature.getArity(), args, sourceLoc);
        }
        return f;
    }

    private AbstractFunctionCallExpression lookupUserDefinedFunction(FunctionSignature signature,
            List<Mutable<ILogicalExpression>> args, SourceLocation sourceLoc) throws CompilationException {
        try {
            if (signature.getNamespace() == null) {
                return null;
            }
            Function function =
                    MetadataManager.INSTANCE.getFunction(metadataProvider.getMetadataTxnContext(), signature);
            if (function == null) {
                return null;
            }
            AbstractFunctionCallExpression f;
            if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_JAVA)) {
                IFunctionInfo finfo = ExternalFunctionCompilerUtil
                        .getExternalFunctionInfo(metadataProvider.getMetadataTxnContext(), function);
                f = new ScalarFunctionCallExpression(finfo, args);
                f.setSourceLocation(sourceLoc);
            } else if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)
                    || function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_SQLPP)) {
                IFunctionInfo finfo = FunctionUtil.getFunctionInfo(signature);
                f = new ScalarFunctionCallExpression(finfo, args);
                f.setSourceLocation(sourceLoc);
            } else {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        " User defined functions written in " + function.getLanguage() + " are not supported");
            }
            return f;
        } catch (AlgebricksException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, e.getMessage(), e);
        }
    }

    private AbstractFunctionCallExpression lookupBuiltinFunction(String functionName, int arity,
            List<Mutable<ILogicalExpression>> args, SourceLocation sourceLoc) {
        AbstractFunctionCallExpression f;
        FunctionIdentifier fi = getBuiltinFunctionIdentifier(functionName, arity);
        if (fi == null) {
            return null;
        }
        if (BuiltinFunctions.isBuiltinAggregateFunction(fi)) {
            f = BuiltinFunctions.makeAggregateFunctionExpression(fi, args);
        } else if (BuiltinFunctions.isBuiltinUnnestingFunction(fi)) {
            UnnestingFunctionCallExpression ufce =
                    new UnnestingFunctionCallExpression(FunctionUtil.getFunctionInfo(fi), args);
            ufce.setReturnsUniqueValues(BuiltinFunctions.returnsUniqueValues(fi));
            f = ufce;
        } else if (BuiltinFunctions.isWindowFunction(fi)) {
            f = BuiltinFunctions.makeWindowFunctionExpression(fi, args);
        } else {
            f = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(fi), args);
        }
        f.setSourceLocation(sourceLoc);
        return f;
    }

    protected FunctionIdentifier getBuiltinFunctionIdentifier(String functionName, int arity) {
        FunctionIdentifier fi = new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, functionName, arity);
        FunctionInfo afi = BuiltinFunctions.lookupFunction(fi);
        FunctionIdentifier builtinAquafi = afi == null ? null : afi.getFunctionIdentifier();

        if (builtinAquafi != null) {
            fi = builtinAquafi;
        } else {
            fi = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, functionName, arity);
            afi = BuiltinFunctions.lookupFunction(fi);
            if (afi == null) {
                return null;
            }
        }
        return fi;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FunctionDecl fd, Mutable<ILogicalOperator> tupSource) {
        throw new IllegalStateException("Function declarations should be inlined at AST rewriting phase.");
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(GroupbyClause gc, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        SourceLocation sourceLoc = gc.getSourceLocation();
        Mutable<ILogicalOperator> topOp = tupSource;
        LogicalVariable groupRecordVar = null;
        if (gc.hasGroupVar()) {
            groupRecordVar = context.newVar();
            AbstractFunctionCallExpression groupRecordConstr =
                    createRecordConstructor(gc.getGroupFieldList(), topOp, sourceLoc);
            AssignOperator groupRecordVarAssignOp =
                    new AssignOperator(groupRecordVar, new MutableObject<>(groupRecordConstr));
            groupRecordVarAssignOp.getInputs().add(topOp);
            groupRecordVarAssignOp.setSourceLocation(sourceLoc);
            topOp = new MutableObject<>(groupRecordVarAssignOp);
        }

        GroupByOperator gOp = new GroupByOperator();
        for (GbyVariableExpressionPair ve : gc.getGbyPairList()) {
            VariableExpr vexpr = ve.getVar();
            LogicalVariable v = vexpr == null ? context.newVar() : context.newVarFromExpression(vexpr);
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(ve.getExpr(), topOp);
            gOp.addGbyExpression(v, eo.first);
            topOp = eo.second;
        }
        if (gc.hasDecorList()) {
            for (GbyVariableExpressionPair ve : gc.getDecorPairList()) {
                VariableExpr vexpr = ve.getVar();
                LogicalVariable v = vexpr == null ? context.newVar() : context.newVarFromExpression(vexpr);
                Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(ve.getExpr(), topOp);
                gOp.addDecorExpression(v, eo.first);
                topOp = eo.second;
            }
        }

        gOp.getInputs().add(topOp);

        if (gc.hasGroupVar()) {
            VariableExpr groupVar = gc.getGroupVar();
            LogicalVariable groupLogicalVar = context.newVar();
            NestedTupleSourceOperator ntsOp = new NestedTupleSourceOperator(new MutableObject<>(gOp));
            ntsOp.setSourceLocation(sourceLoc);
            VariableReferenceExpression groupRecordVarRef = new VariableReferenceExpression(groupRecordVar);
            groupRecordVarRef.setSourceLocation(sourceLoc);
            ILogicalPlan nestedPlan = createNestedPlanWithAggregate(groupLogicalVar, BuiltinFunctions.LISTIFY,
                    groupRecordVarRef, new MutableObject<>(ntsOp));
            gOp.getNestedPlans().add(nestedPlan);
            context.setVar(groupVar, groupLogicalVar);
        }

        if (gc.hasWithMap()) {
            for (Entry<Expression, VariableExpr> entry : gc.getWithVarMap().entrySet()) {
                VariableExpr withVar = entry.getValue();
                Expression withExpr = entry.getKey();
                NestedTupleSourceOperator ntsOp = new NestedTupleSourceOperator(new MutableObject<>(gOp));
                ntsOp.setSourceLocation(sourceLoc);
                Pair<ILogicalExpression, Mutable<ILogicalOperator>> listifyInput =
                        langExprToAlgExpression(withExpr, new MutableObject<>(ntsOp));
                LogicalVariable withLogicalVar = context.newVar();
                ILogicalPlan nestedPlan = createNestedPlanWithAggregate(withLogicalVar, BuiltinFunctions.LISTIFY,
                        listifyInput.first, listifyInput.second);
                gOp.getNestedPlans().add(nestedPlan);
                context.setVar(withVar, withLogicalVar);
            }
        }

        gOp.setGroupAll(gc.isGroupAll());
        gOp.getAnnotations().put(OperatorAnnotations.USE_HASH_GROUP_BY, gc.hasHashGroupByHint());
        gOp.setSourceLocation(sourceLoc);
        return new Pair<>(gOp, null);
    }

    protected AbstractFunctionCallExpression createRecordConstructor(List<Pair<Expression, Identifier>> fieldList,
            Mutable<ILogicalOperator> inputOp, SourceLocation sourceLoc) throws CompilationException {
        List<Mutable<ILogicalExpression>> args = new ArrayList<>();
        for (Pair<Expression, Identifier> field : fieldList) {
            ILogicalExpression fieldNameExpr =
                    langExprToAlgExpression(new LiteralExpr(new StringLiteral(field.second.getValue())), inputOp).first;
            args.add(new MutableObject<>(fieldNameExpr));
            ILogicalExpression fieldExpr = langExprToAlgExpression(field.first, inputOp).first;
            args.add(new MutableObject<>(fieldExpr));
        }
        ScalarFunctionCallExpression recordConstr = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR), args);
        recordConstr.setSourceLocation(sourceLoc);
        return recordConstr;
    }

    protected ILogicalPlan createNestedPlanWithAggregate(LogicalVariable aggOutputVar, FunctionIdentifier aggFunc,
            ILogicalExpression aggFnInput, Mutable<ILogicalOperator> aggOpInput) {
        SourceLocation sourceLoc = aggFnInput.getSourceLocation();
        AggregateFunctionCallExpression aggFnCall = BuiltinFunctions.makeAggregateFunctionExpression(aggFunc,
                mkSingletonArrayList(new MutableObject<>(aggFnInput)));
        aggFnCall.setSourceLocation(sourceLoc);
        AggregateOperator aggOp = new AggregateOperator(mkSingletonArrayList(aggOutputVar),
                mkSingletonArrayList(new MutableObject<>(aggFnCall)));
        aggOp.getInputs().add(aggOpInput);
        aggOp.setSourceLocation(sourceLoc);
        return new ALogicalPlanImpl(new MutableObject<>(aggOp));
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(IfExpr ifexpr, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        // In the most general case, IfThenElse is translated in the following
        // way.
        //
        // We assign the result of the condition to one variable varCond.
        // We create one subplan which contains the plan for the "then" branch,
        // on top of which there is a selection whose condition is varCond.
        // Similarly, we create one subplan for the "else" branch, in which the
        // selection is not(varCond).
        // Finally, we select the desired result.
        Expression condExpr = ifexpr.getCondExpr();
        Expression thenExpr = ifexpr.getThenExpr();
        Expression elseExpr = ifexpr.getElseExpr();

        Pair<ILogicalOperator, LogicalVariable> pCond = condExpr.accept(this, tupSource);
        LogicalVariable varCond = pCond.second;

        // Creates a subplan for the "then" branch.
        VariableReferenceExpression varCondRef1 = new VariableReferenceExpression(varCond);
        varCondRef1.setSourceLocation(condExpr.getSourceLocation());

        Pair<ILogicalOperator, LogicalVariable> opAndVarForThen =
                constructSubplanOperatorForBranch(pCond.first, new MutableObject<>(varCondRef1), thenExpr);

        // Creates a subplan for the "else" branch.
        VariableReferenceExpression varCondRef2 = new VariableReferenceExpression(varCond);
        varCondRef2.setSourceLocation(condExpr.getSourceLocation());
        AbstractFunctionCallExpression notVarCond =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.NOT),
                        Collections.singletonList(generateAndNotIsUnknownWrap(varCondRef2)));
        notVarCond.setSourceLocation(condExpr.getSourceLocation());

        Pair<ILogicalOperator, LogicalVariable> opAndVarForElse =
                constructSubplanOperatorForBranch(opAndVarForThen.first, new MutableObject<>(notVarCond), elseExpr);

        // Uses switch-case function to select the results of two branches.
        LogicalVariable selectVar = context.newVar();
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<>();
        VariableReferenceExpression varCondRef3 = new VariableReferenceExpression(varCond);
        varCondRef3.setSourceLocation(condExpr.getSourceLocation());
        VariableReferenceExpression varThenRef = new VariableReferenceExpression(opAndVarForThen.second);
        varThenRef.setSourceLocation(thenExpr.getSourceLocation());
        VariableReferenceExpression varElseRef = new VariableReferenceExpression(opAndVarForElse.second);
        varElseRef.setSourceLocation(elseExpr.getSourceLocation());
        arguments.add(new MutableObject<>(varCondRef3));
        arguments.add(new MutableObject<>(ConstantExpression.TRUE));
        arguments.add(new MutableObject<>(varThenRef));
        arguments.add(new MutableObject<>(varElseRef));
        AbstractFunctionCallExpression swithCaseExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.SWITCH_CASE), arguments);
        swithCaseExpr.setSourceLocation(ifexpr.getSourceLocation());
        AssignOperator assignOp = new AssignOperator(selectVar, new MutableObject<>(swithCaseExpr));
        assignOp.getInputs().add(new MutableObject<>(opAndVarForElse.first));
        assignOp.setSourceLocation(ifexpr.getSourceLocation());

        // Unnests the selected ("if" or "else") result.
        LogicalVariable unnestVar = context.newVar();
        VariableReferenceExpression selectVarRef = new VariableReferenceExpression(selectVar);
        selectVarRef.setSourceLocation(ifexpr.getSourceLocation());
        UnnestingFunctionCallExpression scanCollExpr =
                new UnnestingFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.SCAN_COLLECTION),
                        Collections.singletonList(new MutableObject<>(selectVarRef)));
        scanCollExpr.setSourceLocation(ifexpr.getSourceLocation());
        UnnestOperator unnestOp = new UnnestOperator(unnestVar, new MutableObject<>(scanCollExpr));
        unnestOp.getInputs().add(new MutableObject<>(assignOp));
        unnestOp.setSourceLocation(ifexpr.getSourceLocation());

        // Produces the final result.
        LogicalVariable resultVar = context.newVar();
        VariableReferenceExpression unnestVarRef = new VariableReferenceExpression(unnestVar);
        unnestVarRef.setSourceLocation(ifexpr.getSourceLocation());
        AssignOperator finalAssignOp = new AssignOperator(resultVar, new MutableObject<>(unnestVarRef));
        finalAssignOp.getInputs().add(new MutableObject<>(unnestOp));
        finalAssignOp.setSourceLocation(ifexpr.getSourceLocation());
        return new Pair<>(finalAssignOp, resultVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(LiteralExpr l, Mutable<ILogicalOperator> tupSource) {
        SourceLocation sourceLoc = l.getSourceLocation();
        LogicalVariable var = context.newVar();
        AsterixConstantValue cValue = new AsterixConstantValue(ConstantHelper.objectFromLiteral(l.getValue()));
        ConstantExpression cExpr = new ConstantExpression(cValue);
        cExpr.setSourceLocation(sourceLoc);
        AssignOperator a = new AssignOperator(var, new MutableObject<>(cExpr));
        a.setSourceLocation(sourceLoc);
        if (tupSource != null) {
            a.getInputs().add(tupSource);
        }
        return new Pair<>(a, var);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(OperatorExpr op, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        List<OperatorType> ops = op.getOpList();
        int nOps = ops.size();

        if (nOps > 0 && (ops.get(0) == OperatorType.AND || ops.get(0) == OperatorType.OR)) {
            return visitAndOrOperator(op, tupSource);
        }

        List<Expression> exprs = op.getExprList();

        Mutable<ILogicalOperator> topOp = tupSource;

        SourceLocation sourceLoc = op.getSourceLocation();
        AbstractFunctionCallExpression currExpr = null;
        for (int i = 0; i <= nOps; i++) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(exprs.get(i), topOp);
            topOp = p.second;
            ILogicalExpression e = p.first;
            // now look at the operator
            if (i < nOps) {
                OperatorType opType = ops.get(i);
                boolean isCmpOp = OperatorExpr.opIsComparison(opType);
                AbstractFunctionCallExpression f = createFunctionCallExpressionForBuiltinOperator(opType, sourceLoc);

                // chain the operators
                if (i == 0) {
                    f.getArguments().add(new MutableObject<>(e));
                    currExpr = f;
                    if (isCmpOp && op.isBroadcastOperand(i)) {
                        BroadcastExpressionAnnotation bcast = new BroadcastExpressionAnnotation();
                        bcast.setObject(BroadcastSide.LEFT);
                        f.getAnnotations().put(BroadcastExpressionAnnotation.BROADCAST_ANNOTATION_KEY, bcast);
                    }
                } else {
                    currExpr.getArguments().add(new MutableObject<>(e));
                    f.getArguments().add(new MutableObject<>(currExpr));
                    currExpr = f;
                    if (isCmpOp && i == 1 && op.isBroadcastOperand(i)) {
                        BroadcastExpressionAnnotation bcast = new BroadcastExpressionAnnotation();
                        bcast.setObject(BroadcastSide.RIGHT);
                        f.getAnnotations().put(BroadcastExpressionAnnotation.BROADCAST_ANNOTATION_KEY, bcast);
                    }
                }
            } else { // don't forget the last expression...
                currExpr.getArguments().add(new MutableObject<>(e));
                if (i == 1 && op.isBroadcastOperand(i)) {
                    BroadcastExpressionAnnotation bcast = new BroadcastExpressionAnnotation();
                    bcast.setObject(BroadcastSide.RIGHT);
                    currExpr.getAnnotations().put(BroadcastExpressionAnnotation.BROADCAST_ANNOTATION_KEY, bcast);
                }
            }
        }

        // Add hints as annotations.
        if (op.hasHints()) {
            for (IExpressionAnnotation hint : op.getHints()) {
                currExpr.getAnnotations().put(hint, hint);
            }
        }

        LogicalVariable assignedVar = context.newVar();
        AssignOperator a = new AssignOperator(assignedVar, new MutableObject<>(currExpr));
        a.getInputs().add(topOp);
        a.setSourceLocation(sourceLoc);
        return new Pair<>(a, assignedVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(OrderbyClause oc, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        SourceLocation sourceLoc = oc.getSourceLocation();
        OrderOperator ord = new OrderOperator();
        ord.setSourceLocation(sourceLoc);
        Iterator<OrderModifier> modifIter = oc.getModifierList().iterator();
        Mutable<ILogicalOperator> topOp = tupSource;
        for (Expression e : oc.getOrderbyList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(e, topOp);
            OrderModifier m = modifIter.next();
            OrderOperator.IOrder comp = translateOrderModifier(m);
            ord.getOrderExpressions().add(new Pair<>(comp, new MutableObject<>(p.first)));
            topOp = p.second;
        }
        ord.getInputs().add(topOp);
        if (oc.getNumTuples() > 0) {
            ord.getAnnotations().put(OperatorAnnotations.CARDINALITY, oc.getNumTuples());
        }
        if (oc.getNumFrames() > 0) {
            ord.getAnnotations().put(OperatorAnnotations.MAX_NUMBER_FRAMES, oc.getNumFrames());
        }
        if (oc.getRangeMap() != null) {
            Iterator<OrderModifier> orderModifIter = oc.getModifierList().iterator();
            boolean ascending = orderModifIter.next() == OrderModifier.ASC;
            RangeMapBuilder.verifyRangeOrder(oc.getRangeMap(), ascending, sourceLoc);
            ord.getAnnotations().put(OperatorAnnotations.USE_STATIC_RANGE, oc.getRangeMap());
        }
        return new Pair<>(ord, null);
    }

    protected OrderOperator.IOrder translateOrderModifier(OrderModifier m) {
        return m == OrderModifier.ASC ? OrderOperator.ASC_ORDER : OrderOperator.DESC_ORDER;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(QuantifiedExpression qe, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        SourceLocation sourceLoc = qe.getSourceLocation();
        Mutable<ILogicalOperator> topOp = tupSource;

        ILogicalOperator firstOp = null;
        Mutable<ILogicalOperator> lastOp = null;

        for (QuantifiedPair qt : qe.getQuantifiedList()) {
            Expression expr = qt.getExpr();
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo1 = langExprToAlgExpression(expr, topOp);
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> pUnnestExpr =
                    makeUnnestExpression(eo1.first, eo1.second);
            topOp = pUnnestExpr.second;
            LogicalVariable uVar = context.newVarFromExpression(qt.getVarExpr());
            UnnestOperator u = new UnnestOperator(uVar, new MutableObject<>(pUnnestExpr.first));
            u.setSourceLocation(expr.getSourceLocation());

            if (firstOp == null) {
                firstOp = u;
            }
            if (lastOp != null) {
                u.getInputs().add(lastOp);
            }
            lastOp = new MutableObject<>(u);
        }

        // We make all the unnest correspond. to quantif. vars. sit on top
        // in the hope of enabling joins & other optimiz.
        firstOp.getInputs().add(topOp);
        topOp = lastOp;

        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo2 = langExprToAlgExpression(qe.getSatisfiesExpr(), topOp);

        AggregateFunctionCallExpression fAgg;
        SelectOperator s;
        if (qe.getQuantifier() == Quantifier.SOME) {
            s = new SelectOperator(new MutableObject<>(eo2.first), false, null);
            s.getInputs().add(eo2.second);
            s.setSourceLocation(sourceLoc);
            fAgg = BuiltinFunctions.makeAggregateFunctionExpression(BuiltinFunctions.NON_EMPTY_STREAM,
                    new ArrayList<>());
            fAgg.setSourceLocation(sourceLoc);
        } else { // EVERY
            // look for input items that do not satisfy the condition, if none found then return true
            // when inverting the condition account for NULL/MISSING by replacing them with FALSE
            // condition() -> not(if-missing-or-null(condition(), false))

            List<Mutable<ILogicalExpression>> ifMissingOrNullArgs = new ArrayList<>(2);
            ConstantExpression eFalse = new ConstantExpression(new AsterixConstantValue(ABoolean.FALSE));
            eFalse.setSourceLocation(sourceLoc);
            ifMissingOrNullArgs.add(new MutableObject<>(eo2.first));
            ifMissingOrNullArgs.add(new MutableObject<>(eFalse));

            List<Mutable<ILogicalExpression>> notArgs = new ArrayList<>(1);
            ScalarFunctionCallExpression ifMissinOrNullExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.IF_MISSING_OR_NULL), ifMissingOrNullArgs);
            ifMissinOrNullExpr.setSourceLocation(sourceLoc);
            notArgs.add(new MutableObject<>(ifMissinOrNullExpr));

            ScalarFunctionCallExpression notExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.NOT), notArgs);
            notExpr.setSourceLocation(sourceLoc);
            s = new SelectOperator(new MutableObject<>(notExpr), false, null);
            s.getInputs().add(eo2.second);
            s.setSourceLocation(sourceLoc);
            fAgg = BuiltinFunctions.makeAggregateFunctionExpression(BuiltinFunctions.EMPTY_STREAM, new ArrayList<>());
            fAgg.setSourceLocation(sourceLoc);
        }
        LogicalVariable qeVar = context.newVar();
        AggregateOperator a =
                new AggregateOperator(mkSingletonArrayList(qeVar), mkSingletonArrayList(new MutableObject<>(fAgg)));
        a.getInputs().add(new MutableObject<>(s));
        a.setSourceLocation(sourceLoc);
        return new Pair<>(a, qeVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(Query q, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        return q.getBody().accept(this, tupSource);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(RecordConstructor rc, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        AbstractFunctionCallExpression f = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR));
        f.setSourceLocation(rc.getSourceLocation());
        LogicalVariable v1 = context.newVar();
        AssignOperator a = new AssignOperator(v1, new MutableObject<>(f));
        a.setSourceLocation(rc.getSourceLocation());
        Mutable<ILogicalOperator> topOp = tupSource;
        for (FieldBinding fb : rc.getFbList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo1 = langExprToAlgExpression(fb.getLeftExpr(), topOp);
            f.getArguments().add(new MutableObject<>(eo1.first));
            topOp = eo1.second;
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo2 = langExprToAlgExpression(fb.getRightExpr(), topOp);
            f.getArguments().add(new MutableObject<>(eo2.first));
            topOp = eo2.second;
        }
        a.getInputs().add(topOp);
        return new Pair<>(a, v1);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(ListConstructor lc, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        SourceLocation sourceLoc = lc.getSourceLocation();
        FunctionIdentifier fid = (lc.getType() == ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR)
                ? BuiltinFunctions.ORDERED_LIST_CONSTRUCTOR : BuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR;
        AbstractFunctionCallExpression f = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(fid));
        f.setSourceLocation(sourceLoc);
        LogicalVariable v1 = context.newVar();
        AssignOperator a = new AssignOperator(v1, new MutableObject<>(f));
        a.setSourceLocation(sourceLoc);
        Mutable<ILogicalOperator> topOp = tupSource;
        for (Expression expr : lc.getExprList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(expr, topOp);
            f.getArguments().add(new MutableObject<>(eo.first));
            topOp = eo.second;
        }
        a.getInputs().add(topOp);
        return new Pair<>(a, v1);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(UnaryExpr u, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        SourceLocation sourceLoc = u.getSourceLocation();
        Expression expr = u.getExpr();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(expr, tupSource);
        LogicalVariable v1 = context.newVar();
        AssignOperator a;
        switch (u.getExprType()) {
            case POSITIVE:
                a = new AssignOperator(v1, new MutableObject<>(eo.first));
                a.setSourceLocation(sourceLoc);
                break;
            case NEGATIVE:
                AbstractFunctionCallExpression m = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.NUMERIC_UNARY_MINUS));
                m.setSourceLocation(sourceLoc);
                m.getArguments().add(new MutableObject<>(eo.first));
                a = new AssignOperator(v1, new MutableObject<>(m));
                a.setSourceLocation(sourceLoc);
                break;
            case EXISTS:
                a = processExists(eo.first, v1, false, sourceLoc);
                break;
            case NOT_EXISTS:
                a = processExists(eo.first, v1, true, sourceLoc);
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Unsupported operator: " + u.getExprType());
        }
        a.getInputs().add(eo.second);
        return new Pair<>(a, v1);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(VariableExpr v, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        // Should we ever get to this method?
        ILogicalExpression oldVRef = translateVariableRef(v);
        LogicalVariable var = context.newVar();
        AssignOperator a = new AssignOperator(var, new MutableObject<>(oldVRef));
        a.getInputs().add(tupSource);
        a.setSourceLocation(v.getSourceLocation());
        return new Pair<>(a, var);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(WhereClause w, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(w.getWhereExpr(), tupSource);
        SelectOperator s = new SelectOperator(new MutableObject<>(p.first), false, null);
        s.getInputs().add(p.second);
        s.setSourceLocation(w.getSourceLocation());
        return new Pair<>(s, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(LimitClause lc, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        SourceLocation sourceLoc = lc.getSourceLocation();
        LimitOperator opLim;

        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p1 = langExprToAlgExpression(lc.getLimitExpr(), tupSource);
        ILogicalExpression maxObjectsExpr =
                createLimitOffsetValueExpression(p1.first, lc.getLimitExpr().getSourceLocation());
        Expression offset = lc.getOffset();
        if (offset != null) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p2 = langExprToAlgExpression(offset, p1.second);
            ILogicalExpression offsetExpr =
                    createLimitOffsetValueExpression(p2.first, lc.getOffset().getSourceLocation());
            opLim = new LimitOperator(maxObjectsExpr, offsetExpr);
            opLim.getInputs().add(p2.second);
            opLim.setSourceLocation(sourceLoc);
        } else {
            opLim = new LimitOperator(maxObjectsExpr);
            opLim.getInputs().add(p1.second);
            opLim.setSourceLocation(sourceLoc);
        }
        return new Pair<>(opLim, null);
    }

    private ILogicalExpression createLimitOffsetValueExpression(ILogicalExpression inputExpr, SourceLocation sourceLoc)
            throws CompilationException {
        // generates expression for limit and offset value:
        //
        // switch-case(treat-as-integer(user_value_expr) > 0, true, treat-as-integer(user_value_expr), 0)
        //
        // this guarantees that the value is always an integer and greater or equals to 0,
        // so CopyLimitDownRule works correctly when computing the total limit,
        // and other rules which assume integer type

        AInt32 zero = new AInt32(0);

        AbstractFunctionCallExpression valueExpr =
                createFunctionCallExpression(BuiltinFunctions.TREAT_AS_INTEGER, sourceLoc);
        valueExpr.getArguments().add(new MutableObject<>(inputExpr));

        AbstractFunctionCallExpression cmpExpr =
                createFunctionCallExpressionForBuiltinOperator(OperatorType.GT, sourceLoc);
        cmpExpr.getArguments().add(new MutableObject<>(valueExpr));
        cmpExpr.getArguments().add(new MutableObject<>(createConstantExpression(zero, sourceLoc)));

        AbstractFunctionCallExpression switchExpr =
                createFunctionCallExpression(BuiltinFunctions.SWITCH_CASE, sourceLoc);
        switchExpr.getArguments().add(new MutableObject<>(cmpExpr));
        switchExpr.getArguments().add(new MutableObject<>(createConstantExpression(ABoolean.TRUE, sourceLoc)));
        switchExpr.getArguments().add(new MutableObject<>(valueExpr.cloneExpression()));
        switchExpr.getArguments().add(new MutableObject<>(createConstantExpression(zero, sourceLoc)));

        return switchExpr;
    }

    protected static AbstractFunctionCallExpression createFunctionCallExpressionForBuiltinOperator(OperatorType t,
            SourceLocation sourceLoc) throws CompilationException {
        FunctionIdentifier fid;
        switch (t) {
            case EQ:
                fid = AlgebricksBuiltinFunctions.EQ;
                break;
            case NEQ:
                fid = AlgebricksBuiltinFunctions.NEQ;
                break;
            case GT:
                fid = AlgebricksBuiltinFunctions.GT;
                break;
            case GE:
                fid = AlgebricksBuiltinFunctions.GE;
                break;
            case LT:
                fid = AlgebricksBuiltinFunctions.LT;
                break;
            case LE:
                fid = AlgebricksBuiltinFunctions.LE;
                break;
            case PLUS:
                fid = AlgebricksBuiltinFunctions.NUMERIC_ADD;
                break;
            case MINUS:
                fid = BuiltinFunctions.NUMERIC_SUBTRACT;
                break;
            case MUL:
                fid = BuiltinFunctions.NUMERIC_MULTIPLY;
                break;
            case DIVIDE:
                fid = BuiltinFunctions.NUMERIC_DIVIDE;
                break;
            case DIV:
                fid = BuiltinFunctions.NUMERIC_DIV;
                break;
            case MOD:
                fid = BuiltinFunctions.NUMERIC_MOD;
                break;
            case CARET:
                fid = BuiltinFunctions.NUMERIC_POWER;
                break;
            case AND:
                fid = AlgebricksBuiltinFunctions.AND;
                break;
            case OR:
                fid = AlgebricksBuiltinFunctions.OR;
                break;
            case FUZZY_EQ:
                fid = BuiltinFunctions.FUZZY_EQ;
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Operator " + t + " is not yet implemented");
        }
        return createFunctionCallExpression(fid, sourceLoc);
    }

    protected static AbstractFunctionCallExpression createFunctionCallExpression(FunctionIdentifier fid,
            SourceLocation sourceLoc) {
        ScalarFunctionCallExpression callExpr = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(fid));
        callExpr.setSourceLocation(sourceLoc);
        return callExpr;
    }

    protected Pair<ILogicalExpression, Mutable<ILogicalOperator>> langExprToAlgExpression(Expression expr,
            Mutable<ILogicalOperator> topOpRef) throws CompilationException {
        SourceLocation sourceLoc = expr.getSourceLocation();
        switch (expr.getKind()) {
            case VARIABLE_EXPRESSION:
                VariableExpr varExpr = (VariableExpr) expr;
                ILogicalExpression varRefExpr = translateVariableRef(varExpr);
                return new Pair<>(varRefExpr, topOpRef);
            case LITERAL_EXPRESSION:
                LiteralExpr val = (LiteralExpr) expr;
                AsterixConstantValue cValue =
                        new AsterixConstantValue(ConstantHelper.objectFromLiteral(val.getValue()));
                ConstantExpression cExpr = new ConstantExpression(cValue);
                cExpr.setSourceLocation(sourceLoc);
                return new Pair<>(cExpr, topOpRef);
            default:
                if (expressionNeedsNoNesting(expr)) {
                    Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this, topOpRef);
                    ILogicalExpression exp = ((AssignOperator) p.first).getExpressions().get(0).getValue();
                    return new Pair<>(exp, p.first.getInputs().get(0));
                } else {
                    Mutable<ILogicalOperator> srcRef = new MutableObject<>();
                    Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this, srcRef);
                    if (p.first.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                        if (topOpRef.getValue() != null) {
                            srcRef.setValue(topOpRef.getValue());
                        } else {
                            // Re-binds the bottom operator reference to {@code topOpRef}.
                            rebindBottomOpRef(p.first, srcRef, topOpRef);
                        }
                        Mutable<ILogicalOperator> top2 = new MutableObject<>(p.first);
                        VariableReferenceExpression varRef = new VariableReferenceExpression(p.second);
                        varRef.setSourceLocation(sourceLoc);
                        return new Pair<>(varRef, top2);
                    } else {
                        SubplanOperator s = new SubplanOperator();
                        s.getInputs().add(topOpRef);
                        s.setSourceLocation(sourceLoc);
                        NestedTupleSourceOperator ntsOp = new NestedTupleSourceOperator(new MutableObject<>(s));
                        ntsOp.setSourceLocation(sourceLoc);
                        srcRef.setValue(ntsOp);
                        Mutable<ILogicalOperator> planRoot = new MutableObject<>(p.first);
                        s.setRootOp(planRoot);
                        VariableReferenceExpression varRef = new VariableReferenceExpression(p.second);
                        varRef.setSourceLocation(sourceLoc);
                        return new Pair<>(varRef, new MutableObject<>(s));
                    }
                }
        }
    }

    protected Pair<ILogicalOperator, LogicalVariable> aggListifyForSubquery(LogicalVariable var,
            Mutable<ILogicalOperator> opRef, boolean bProject) {
        SourceLocation sourceLoc = opRef.getValue().getSourceLocation();
        AggregateFunctionCallExpression funAgg =
                BuiltinFunctions.makeAggregateFunctionExpression(BuiltinFunctions.LISTIFY, new ArrayList<>());
        funAgg.getArguments().add(new MutableObject<>(new VariableReferenceExpression(var)));
        funAgg.setSourceLocation(sourceLoc);

        LogicalVariable varListified = context.newSubplanOutputVar();
        AggregateOperator agg = new AggregateOperator(mkSingletonArrayList(varListified),
                mkSingletonArrayList(new MutableObject<>(funAgg)));
        agg.getInputs().add(opRef);
        agg.setSourceLocation(sourceLoc);
        ILogicalOperator res;
        if (bProject) {
            ProjectOperator pr = new ProjectOperator(varListified);
            pr.getInputs().add(new MutableObject<>(agg));
            pr.setSourceLocation(sourceLoc);
            res = pr;
        } else {
            res = agg;
        }
        return new Pair<>(res, varListified);
    }

    protected Pair<ILogicalOperator, LogicalVariable> visitAndOrOperator(OperatorExpr op,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        SourceLocation sourceLoc = op.getSourceLocation();
        List<OperatorType> ops = op.getOpList();
        int nOps = ops.size();

        List<Expression> exprs = op.getExprList();

        Mutable<ILogicalOperator> topOp = tupSource;

        OperatorType opLogical = ops.get(0);
        AbstractFunctionCallExpression f = createFunctionCallExpressionForBuiltinOperator(opLogical, sourceLoc);

        for (int i = 0; i <= nOps; i++) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(exprs.get(i), topOp);
            topOp = p.second;
            // now look at the operator
            if (i < nOps && ops.get(i) != opLogical) {
                throw new CompilationException(ErrorCode.COMPILATION_UNEXPECTED_OPERATOR, sourceLoc, ops.get(i),
                        opLogical);
            }
            f.getArguments().add(new MutableObject<>(p.first));
        }

        LogicalVariable assignedVar = context.newVar();
        AssignOperator a = new AssignOperator(assignedVar, new MutableObject<>(f));
        a.getInputs().add(topOp);
        a.setSourceLocation(sourceLoc);

        return new Pair<>(a, assignedVar);

    }

    protected boolean expressionNeedsNoNesting(Expression expr) throws CompilationException {
        Kind k = expr.getKind();
        boolean noNesting = k == Kind.LITERAL_EXPRESSION || k == Kind.LIST_CONSTRUCTOR_EXPRESSION
                || k == Kind.RECORD_CONSTRUCTOR_EXPRESSION || k == Kind.VARIABLE_EXPRESSION;
        noNesting = noNesting || k == Kind.CALL_EXPRESSION || k == Kind.OP_EXPRESSION
                || k == Kind.FIELD_ACCESSOR_EXPRESSION;
        noNesting = noNesting || k == Kind.INDEX_ACCESSOR_EXPRESSION || k == Kind.UNARY_EXPRESSION
                || k == Kind.IF_EXPRESSION;
        return noNesting || k == Kind.CASE_EXPRESSION || k == Kind.WINDOW_EXPRESSION;
    }

    protected <T> List<T> mkSingletonArrayList(T item) {
        ArrayList<T> array = new ArrayList<>(1);
        array.add(item);
        return array;
    }

    protected Pair<ILogicalExpression, Mutable<ILogicalOperator>> makeUnnestExpression(ILogicalExpression expr,
            Mutable<ILogicalOperator> topOpRef) throws CompilationException {
        SourceLocation sourceLoc = expr.getSourceLocation();
        switch (expr.getExpressionTag()) {
            case CONSTANT:
            case VARIABLE:
                UnnestingFunctionCallExpression scanCollExpr1 = new UnnestingFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.SCAN_COLLECTION),
                        mkSingletonArrayList(new MutableObject<>(expr)));
                scanCollExpr1.setSourceLocation(sourceLoc);
                return new Pair<>(scanCollExpr1, topOpRef);
            case FUNCTION_CALL:
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                if (fce.getKind() == FunctionKind.UNNEST) {
                    return new Pair<>(expr, topOpRef);
                } else if (fce.getKind() == FunctionKind.SCALAR && unnestNeedsAssign(fce)) {
                    LogicalVariable var = context.newVar();
                    AssignOperator assignOp = new AssignOperator(var, new MutableObject<>(expr));
                    assignOp.setSourceLocation(sourceLoc);
                    assignOp.getInputs().add(topOpRef);
                    VariableReferenceExpression varRef = new VariableReferenceExpression(var);
                    varRef.setSourceLocation(sourceLoc);
                    UnnestingFunctionCallExpression scanCollExpr2 = new UnnestingFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(BuiltinFunctions.SCAN_COLLECTION),
                            mkSingletonArrayList(new MutableObject<>(varRef)));
                    scanCollExpr2.setSourceLocation(sourceLoc);
                    return new Pair<>(scanCollExpr2, new MutableObject<>(assignOp));
                } else {
                    UnnestingFunctionCallExpression scanCollExpr3 = new UnnestingFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(BuiltinFunctions.SCAN_COLLECTION),
                            mkSingletonArrayList(new MutableObject<>(expr)));
                    scanCollExpr3.setSourceLocation(sourceLoc);
                    return new Pair<>(scanCollExpr3, topOpRef);
                }
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc);
        }
    }

    /**
     * Whether an Assign operator needs to be introduced when unnesting this function call expression.
     */
    private boolean unnestNeedsAssign(AbstractFunctionCallExpression fce) {
        return BuiltinFunctions.getAggregateFunction(fce.getFunctionIdentifier()) != null;
    }

    private boolean rebindBottomOpRef(ILogicalOperator currentOp, Mutable<ILogicalOperator> opRef,
            Mutable<ILogicalOperator> replacementOpRef) {
        int index = 0;
        for (Mutable<ILogicalOperator> childRef : currentOp.getInputs()) {
            if (childRef == opRef) {
                currentOp.getInputs().set(index, replacementOpRef);
                return true;
            } else {
                if (rebindBottomOpRef(childRef.getValue(), opRef, replacementOpRef)) {
                    return true;
                }
            }
            ++index;
        }
        return false;
    }

    /**
     * Eliminate shared operator references in a query plan. Deep copy a new query
     * plan subtree whenever there is a shared operator reference.
     *
     * @param plan,
     *            the query plan.
     * @throws CompilationException
     */
    private void eliminateSharedOperatorReferenceForPlan(ILogicalPlan plan) throws CompilationException {
        for (Mutable<ILogicalOperator> opRef : plan.getRoots()) {
            Set<Mutable<ILogicalOperator>> opRefSet = new HashSet<>();
            eliminateSharedOperatorReference(opRef, opRefSet);
        }
    }

    /**
     * Eliminate shared operator references in a query plan rooted at
     * <code>currentOpRef.getValue()</code>. Deep copy a new query plan subtree
     * whenever there is a shared operator reference.
     *
     * @param currentOpRef,
     *            the operator reference to consider
     * @param opRefSet,
     *            the set storing seen operator references so far.
     * @return a mapping that maps old variables to new variables, for the ancestors
     *         of <code>currentOpRef</code> to replace variables properly.
     * @throws CompilationException
     */
    private LinkedHashMap<LogicalVariable, LogicalVariable> eliminateSharedOperatorReference(
            Mutable<ILogicalOperator> currentOpRef, Set<Mutable<ILogicalOperator>> opRefSet)
            throws CompilationException {
        try {
            opRefSet.add(currentOpRef);
            AbstractLogicalOperator currentOperator = (AbstractLogicalOperator) currentOpRef.getValue();

            // Recursively eliminates shared references in nested plans.
            if (currentOperator.hasNestedPlans()) {
                // Since a nested plan tree itself can never be shared with another nested plan
                // tree in
                // another operator, the operation called in the if block does not need to
                // replace
                // any variables further for <code>currentOpRef.getValue()</code> nor its
                // ancestor.
                AbstractOperatorWithNestedPlans opWithNestedPlan = (AbstractOperatorWithNestedPlans) currentOperator;
                for (ILogicalPlan plan : opWithNestedPlan.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> rootRef : plan.getRoots()) {
                        Set<Mutable<ILogicalOperator>> nestedOpRefSet = new HashSet<>();
                        eliminateSharedOperatorReference(rootRef, nestedOpRefSet);
                    }
                }
            }

            int childIndex = 0;
            LinkedHashMap<LogicalVariable, LogicalVariable> varMap = new LinkedHashMap<>();
            for (Mutable<ILogicalOperator> childRef : currentOperator.getInputs()) {
                if (opRefSet.contains(childRef)) {
                    // There is a shared operator reference in the query plan.
                    // Deep copies the child plan.
                    LogicalOperatorDeepCopyWithNewVariablesVisitor visitor =
                            new LogicalOperatorDeepCopyWithNewVariablesVisitor(context, null);
                    ILogicalOperator newChild = childRef.getValue().accept(visitor, null);
                    LinkedHashMap<LogicalVariable, LogicalVariable> cloneVarMap =
                            visitor.getInputToOutputVariableMapping();

                    // Substitute variables according to the deep copy which generates new
                    // variables.
                    VariableUtilities.substituteVariables(currentOperator, cloneVarMap, null);
                    varMap.putAll(cloneVarMap);

                    // Sets the new child.
                    childRef = new MutableObject<>(newChild);
                    currentOperator.getInputs().set(childIndex, childRef);
                }

                // Recursively eliminate shared operator reference for the operator subtree,
                // even if it is a deep copy of some other one.
                LinkedHashMap<LogicalVariable, LogicalVariable> childVarMap =
                        eliminateSharedOperatorReference(childRef, opRefSet);
                // Substitute variables according to the new subtree.
                VariableUtilities.substituteVariables(currentOperator, childVarMap, null);

                // Updates mapping like <$a, $b> in varMap to <$a, $c>, where there is a mapping
                // <$b, $c>
                // in childVarMap.
                varMap.entrySet().forEach(entry -> {
                    LogicalVariable newVar = childVarMap.get(entry.getValue());
                    if (newVar != null) {
                        entry.setValue(newVar);
                    }
                });
                varMap.putAll(childVarMap);
                ++childIndex;
            }

            // Only retain live variables for parent operators to substitute variables.
            Set<LogicalVariable> liveVars = new HashSet<>();
            VariableUtilities.getLiveVariables(currentOperator, liveVars);
            varMap.values().retainAll(liveVars);
            return varMap;
        } catch (AlgebricksException e) {
            throw new CompilationException(e);
        }
    }

    /**
     * Constructs a subplan operator for a branch in a if-else (or case) expression.
     *
     * @param inputOp,
     *            the input operator.
     * @param selectExpr,
     *            the expression to select tuples that are processed by this branch.
     * @param branchExpression,
     *            the expression to be evaluated in this branch.
     * @return a pair of the constructed subplan operator and the output variable
     *         for the branch.
     * @throws CompilationException
     */
    protected Pair<ILogicalOperator, LogicalVariable> constructSubplanOperatorForBranch(ILogicalOperator inputOp,
            Mutable<ILogicalExpression> selectExpr, Expression branchExpression) throws CompilationException {
        context.enterSubplan();
        SourceLocation sourceLoc = inputOp.getSourceLocation();
        SubplanOperator subplanOp = new SubplanOperator();
        subplanOp.getInputs().add(new MutableObject<>(inputOp));
        subplanOp.setSourceLocation(sourceLoc);
        NestedTupleSourceOperator ntsOp = new NestedTupleSourceOperator(new MutableObject<>(subplanOp));
        ntsOp.setSourceLocation(sourceLoc);
        SelectOperator select = new SelectOperator(selectExpr, false, null);
        // The select operator cannot be moved up and down, otherwise it will cause
        // typing issues (ASTERIXDB-1203).
        OperatorPropertiesUtil.markMovable(select, false);
        select.getInputs().add(new MutableObject<>(ntsOp));
        select.setSourceLocation(selectExpr.getValue().getSourceLocation());

        Pair<ILogicalOperator, LogicalVariable> pBranch = branchExpression.accept(this, new MutableObject<>(select));
        LogicalVariable branchVar = context.newVar();
        VariableReferenceExpression pBranchVarRef = new VariableReferenceExpression(pBranch.second);
        pBranchVarRef.setSourceLocation(branchExpression.getSourceLocation());
        AggregateFunctionCallExpression listifyExpr =
                new AggregateFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.LISTIFY), false,
                        Collections.singletonList(new MutableObject<>(pBranchVarRef)));
        listifyExpr.setSourceLocation(branchExpression.getSourceLocation());
        AggregateOperator aggOp = new AggregateOperator(Collections.singletonList(branchVar),
                Collections.singletonList(new MutableObject<>(listifyExpr)));
        aggOp.getInputs().add(new MutableObject<>(pBranch.first));
        aggOp.setSourceLocation(branchExpression.getSourceLocation());
        ILogicalPlan planForBranch = new ALogicalPlanImpl(new MutableObject<>(aggOp));
        subplanOp.getNestedPlans().add(planForBranch);
        context.exitSubplan();
        return new Pair<>(subplanOp, branchVar);
    }

    // Processes EXISTS and NOT EXISTS.
    private AssignOperator processExists(ILogicalExpression inputExpr, LogicalVariable v1, boolean not,
            SourceLocation sourceLoc) {
        AbstractFunctionCallExpression count =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.SCALAR_COUNT));
        count.getArguments().add(new MutableObject<>(inputExpr));
        count.setSourceLocation(sourceLoc);
        AbstractFunctionCallExpression comparison = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(not ? BuiltinFunctions.EQ : BuiltinFunctions.NEQ));
        ConstantExpression eZero = new ConstantExpression(new AsterixConstantValue(new AInt64(0L)));
        eZero.setSourceLocation(sourceLoc);
        comparison.getArguments().add(new MutableObject<>(count));
        comparison.getArguments().add(new MutableObject<>(eZero));
        comparison.setSourceLocation(sourceLoc);
        AssignOperator a = new AssignOperator(v1, new MutableObject<>(comparison));
        a.setSourceLocation(sourceLoc);
        return a;
    }

    // Generates the filter condition for whether a conditional branch should be
    // executed.
    protected Mutable<ILogicalExpression> generateNoMatchedPrecedingWhenBranchesFilter(
            List<ILogicalExpression> inputBooleanExprs, SourceLocation sourceLoc) {
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<>();
        for (ILogicalExpression inputBooleanExpr : inputBooleanExprs) {
            // A NULL/MISSING valued WHEN expression does not lead to the corresponding THEN
            // execution.
            // Therefore, we should check a previous WHEN boolean condition is not unknown.
            arguments.add(generateAndNotIsUnknownWrap(inputBooleanExpr));
        }
        ScalarFunctionCallExpression hasBeenExecutedExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.OR), arguments);
        hasBeenExecutedExpr.setSourceLocation(sourceLoc);
        ScalarFunctionCallExpression notExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.NOT),
                        new ArrayList<>(Collections.singletonList(new MutableObject<>(hasBeenExecutedExpr))));
        notExpr.setSourceLocation(sourceLoc);
        return new MutableObject<>(notExpr);
    }

    // For an input expression `expr`, return `expr AND expr IS NOT UNKOWN`.
    protected Mutable<ILogicalExpression> generateAndNotIsUnknownWrap(ILogicalExpression logicalExpr) {
        SourceLocation sourceLoc = logicalExpr.getSourceLocation();
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<>();
        arguments.add(new MutableObject<>(logicalExpr));
        ScalarFunctionCallExpression isUnknownExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.IS_UNKNOWN),
                        new ArrayList<>(Collections.singletonList(new MutableObject<>(logicalExpr))));
        isUnknownExpr.setSourceLocation(sourceLoc);
        ScalarFunctionCallExpression notExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.NOT),
                        new ArrayList<>(Collections.singletonList(new MutableObject<>(isUnknownExpr))));
        notExpr.setSourceLocation(sourceLoc);
        arguments.add(new MutableObject<>(notExpr));
        ScalarFunctionCallExpression andExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.AND), arguments);
        andExpr.setSourceLocation(sourceLoc);
        return new MutableObject<>(andExpr);
    }

    // Generates the plan for "UNION ALL" or union expression from its input
    // expressions.
    protected Pair<ILogicalOperator, LogicalVariable> translateUnionAllFromInputExprs(List<ILangExpression> inputExprs,
            Mutable<ILogicalOperator> tupSource, SourceLocation sourceLoc) throws CompilationException {
        List<Mutable<ILogicalOperator>> inputOpRefsToUnion = new ArrayList<>();
        List<LogicalVariable> vars = new ArrayList<>();
        for (ILangExpression expr : inputExprs) {
            SourceLocation exprSourceLoc = expr.getSourceLocation();
            // Visits the expression of one branch.
            Pair<ILogicalOperator, LogicalVariable> opAndVar = expr.accept(this, tupSource);

            // Creates an unnest operator.
            LogicalVariable unnestVar = context.newVar();
            List<Mutable<ILogicalExpression>> args = new ArrayList<>();
            VariableReferenceExpression varRef = new VariableReferenceExpression(opAndVar.second);
            varRef.setSourceLocation(exprSourceLoc);
            args.add(new MutableObject<>(varRef));
            UnnestingFunctionCallExpression scanCollExpr = new UnnestingFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.SCAN_COLLECTION), args);
            scanCollExpr.setSourceLocation(exprSourceLoc);
            UnnestOperator unnestOp = new UnnestOperator(unnestVar, new MutableObject<>(scanCollExpr));
            unnestOp.getInputs().add(new MutableObject<>(opAndVar.first));
            unnestOp.setSourceLocation(exprSourceLoc);
            inputOpRefsToUnion.add(new MutableObject<>(unnestOp));
            vars.add(unnestVar);
        }

        // Creates a tree of binary union-all operators.
        UnionAllOperator topUnionAllOp = null;
        LogicalVariable topUnionVar = null;
        Iterator<Mutable<ILogicalOperator>> inputOpRefIterator = inputOpRefsToUnion.iterator();
        Mutable<ILogicalOperator> leftInputBranch = inputOpRefIterator.next();
        Iterator<LogicalVariable> inputVarIterator = vars.iterator();
        LogicalVariable leftInputVar = inputVarIterator.next();

        while (inputOpRefIterator.hasNext()) {
            // Generates the variable triple <leftVar, rightVar, outputVar> .
            topUnionVar = context.newVar();
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> varTriple =
                    new Triple<>(leftInputVar, inputVarIterator.next(), topUnionVar);
            List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varTriples = new ArrayList<>();
            varTriples.add(varTriple);

            // Creates a binary union-all operator.
            topUnionAllOp = new UnionAllOperator(varTriples);
            topUnionAllOp.getInputs().add(leftInputBranch);
            topUnionAllOp.getInputs().add(inputOpRefIterator.next());
            topUnionAllOp.setSourceLocation(sourceLoc);

            // Re-assigns leftInputBranch and leftInputVar.
            leftInputBranch = new MutableObject<>(topUnionAllOp);
            leftInputVar = topUnionVar;
        }
        return new Pair<>(topUnionAllOp, topUnionVar);
    }

    private ConstantExpression createConstantExpression(IAObject value, SourceLocation sourceLoc) {
        ConstantExpression constExpr = new ConstantExpression(new AsterixConstantValue(value));
        constExpr.setSourceLocation(sourceLoc);
        return constExpr;
    }
}
