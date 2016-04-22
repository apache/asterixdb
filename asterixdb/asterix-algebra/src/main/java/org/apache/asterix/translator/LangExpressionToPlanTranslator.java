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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslator;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.aql.util.RangeMapBuilder;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
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
import org.apache.asterix.lang.common.expression.ListConstructor.Type;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.QuantifiedExpression.Quantifier;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.UnaryExpr.Sign;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.visitor.base.AbstractQueryExpressionVisitor;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.declared.AqlDataSource.AqlDataSourceType;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.LoadableDataSource;
import org.apache.asterix.metadata.declared.ResultSetDataSink;
import org.apache.asterix.metadata.declared.ResultSetSinkId;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.functions.ExternalFunctionCompilerUtil;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.AsterixFunctionInfo;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.runtime.formats.FormatUtils;
import org.apache.asterix.translator.CompiledStatements.CompiledLoadFromFileStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledSubscribeFeedStatement;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.asterix.translator.util.FunctionCollection;
import org.apache.asterix.translator.util.PlanTranslationUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Pair;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalOperatorDeepCopyWithNewVariablesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.std.file.FileSplit;

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

    protected final AqlMetadataProvider metadataProvider;
    protected final TranslationContext context;
    private static final AtomicLong outputFileID = new AtomicLong(0);
    private static final String OUTPUT_FILE_PREFIX = "OUTPUT_";

    public LangExpressionToPlanTranslator(AqlMetadataProvider metadataProvider, int currentVarCounter)
            throws AlgebricksException {
        this.context = new TranslationContext(new Counter(currentVarCounter));
        this.metadataProvider = metadataProvider;
        FormatUtils.getDefaultFormat().registerRuntimeFunctions(FunctionCollection.getFunctionDescriptorFactories());
    }

    @Override
    public int getVarCounter() {
        return context.getVarCounter();
    }

    @Override
    public ILogicalPlan translateLoad(ICompiledDmlStatement stmt) throws AlgebricksException {
        CompiledLoadFromFileStatement clffs = (CompiledLoadFromFileStatement) stmt;
        Dataset dataset = metadataProvider.findDataset(clffs.getDataverseName(), clffs.getDatasetName());
        if (dataset == null) {
            // This would never happen since we check for this in AqlTranslator
            throw new AlgebricksException(
                    "Unable to load dataset " + clffs.getDatasetName() + " since it does not exist");
        }
        IAType itemType = metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        IAType metaItemType = metadataProvider.findType(dataset.getMetaItemTypeDataverseName(),
                dataset.getMetaItemTypeName());
        DatasetDataSource targetDatasource = validateDatasetInfo(metadataProvider, stmt.getDataverseName(),
                stmt.getDatasetName());
        List<List<String>> partitionKeys = DatasetUtils.getPartitioningKeys(targetDatasource.getDataset());
        if (dataset.hasMetaPart()) {
            throw new AlgebricksException(
                    dataset.getDatasetName() + ": load dataset is not supported on Datasets with Meta records");
        }

        LoadableDataSource lds;
        try {
            lds = new LoadableDataSource(dataset, itemType, metaItemType, clffs.getAdapter(), clffs.getProperties());
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }

        // etsOp is a dummy input operator used to keep the compiler happy. it
        // could be removed but would result in
        // the need to fix many rewrite rules that assume that datasourcescan
        // operators always have input.
        ILogicalOperator etsOp = new EmptyTupleSourceOperator();

        // Add a logical variable for the record.
        List<LogicalVariable> payloadVars = new ArrayList<LogicalVariable>();
        payloadVars.add(context.newVar());

        // Create a scan operator and make the empty tuple source its input
        DataSourceScanOperator dssOp = new DataSourceScanOperator(payloadVars, lds);
        dssOp.getInputs().add(new MutableObject<ILogicalOperator>(etsOp));
        ILogicalExpression payloadExpr = new VariableReferenceExpression(payloadVars.get(0));
        Mutable<ILogicalExpression> payloadRef = new MutableObject<ILogicalExpression>(payloadExpr);

        // Creating the assign to extract the PK out of the record
        ArrayList<LogicalVariable> pkVars = new ArrayList<LogicalVariable>();
        ArrayList<Mutable<ILogicalExpression>> pkExprs = new ArrayList<Mutable<ILogicalExpression>>();
        List<Mutable<ILogicalExpression>> varRefsForLoading = new ArrayList<Mutable<ILogicalExpression>>();
        LogicalVariable payloadVar = payloadVars.get(0);
        for (List<String> keyFieldName : partitionKeys) {
            PlanTranslationUtil.prepareVarAndExpression(keyFieldName, payloadVar, pkVars, pkExprs, varRefsForLoading,
                    context);
        }

        AssignOperator assign = new AssignOperator(pkVars, pkExprs);
        assign.getInputs().add(new MutableObject<ILogicalOperator>(dssOp));

        // If the input is pre-sorted, we set the ordering property explicitly in the assign
        if (clffs.alreadySorted()) {
            List<OrderColumn> orderColumns = new ArrayList<OrderColumn>();
            for (int i = 0; i < pkVars.size(); ++i) {
                orderColumns.add(new OrderColumn(pkVars.get(i), OrderKind.ASC));
            }
            assign.setExplicitOrderingProperty(new LocalOrderProperty(orderColumns));
        }

        List<String> additionalFilteringField = DatasetUtils.getFilterField(targetDatasource.getDataset());
        List<LogicalVariable> additionalFilteringVars = null;
        List<Mutable<ILogicalExpression>> additionalFilteringAssignExpressions = null;
        List<Mutable<ILogicalExpression>> additionalFilteringExpressions = null;
        AssignOperator additionalFilteringAssign = null;
        if (additionalFilteringField != null) {
            additionalFilteringVars = new ArrayList<LogicalVariable>();
            additionalFilteringAssignExpressions = new ArrayList<Mutable<ILogicalExpression>>();
            additionalFilteringExpressions = new ArrayList<Mutable<ILogicalExpression>>();
            PlanTranslationUtil.prepareVarAndExpression(additionalFilteringField, payloadVar, additionalFilteringVars,
                    additionalFilteringAssignExpressions, additionalFilteringExpressions, context);
            additionalFilteringAssign = new AssignOperator(additionalFilteringVars,
                    additionalFilteringAssignExpressions);
        }

        InsertDeleteUpsertOperator insertOp = new InsertDeleteUpsertOperator(targetDatasource, payloadRef,
                varRefsForLoading, InsertDeleteUpsertOperator.Kind.INSERT, true);
        insertOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);

        if (additionalFilteringAssign != null) {
            additionalFilteringAssign.getInputs().add(new MutableObject<ILogicalOperator>(assign));
            insertOp.getInputs().add(new MutableObject<ILogicalOperator>(additionalFilteringAssign));
        } else {
            insertOp.getInputs().add(new MutableObject<ILogicalOperator>(assign));
        }

        ILogicalOperator leafOperator = new SinkOperator();
        leafOperator.getInputs().add(new MutableObject<ILogicalOperator>(insertOp));
        return new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(leafOperator));
    }

    @SuppressWarnings("unchecked")
    @Override
    public ILogicalPlan translate(Query expr, String outputDatasetName, ICompiledDmlStatement stmt)
            throws AlgebricksException, AsterixException {
        Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this,
                new MutableObject<ILogicalOperator>(new EmptyTupleSourceOperator()));
        ArrayList<Mutable<ILogicalOperator>> globalPlanRoots = new ArrayList<Mutable<ILogicalOperator>>();
        ILogicalOperator topOp = p.first;
        ProjectOperator project = (ProjectOperator) topOp;
        LogicalVariable unnestVar = project.getVariables().get(0);
        LogicalVariable resVar = project.getVariables().get(0);

        if (outputDatasetName == null) {
            FileSplit outputFileSplit = metadataProvider.getOutputFile();
            if (outputFileSplit == null) {
                outputFileSplit = getDefaultOutputFileLocation();
            }
            metadataProvider.setOutputFile(outputFileSplit);

            List<Mutable<ILogicalExpression>> writeExprList = new ArrayList<Mutable<ILogicalExpression>>(1);
            writeExprList.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(resVar)));
            ResultSetSinkId rssId = new ResultSetSinkId(metadataProvider.getResultSetId());
            ResultSetDataSink sink = new ResultSetDataSink(rssId, null);
            topOp = new DistributeResultOperator(writeExprList, sink);
            topOp.getInputs().add(new MutableObject<ILogicalOperator>(project));

            // Retrieve the Output RecordType (if any) and store it on
            // the DistributeResultOperator
            IAType outputRecordType = metadataProvider.findOutputRecordType();
            if (outputRecordType != null) {
                topOp.getAnnotations().put("output-record-type", outputRecordType);
            }
        } else {
            /**
             * add the collection-to-sequence right before the project,
             * because dataset only accept non-collection records
             */
            LogicalVariable seqVar = context.newVar();
            /** This assign adds a marker function collection-to-sequence: if the input is a singleton collection, unnest it; otherwise do nothing. */
            AssignOperator assignCollectionToSequence = new AssignOperator(seqVar,
                    new MutableObject<ILogicalExpression>(new ScalarFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.COLLECTION_TO_SEQUENCE),
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(resVar)))));
            assignCollectionToSequence.getInputs()
                    .add(new MutableObject<ILogicalOperator>(project.getInputs().get(0).getValue()));
            project.getInputs().get(0).setValue(assignCollectionToSequence);
            project.getVariables().set(0, seqVar);
            resVar = seqVar;
            DatasetDataSource targetDatasource = validateDatasetInfo(metadataProvider, stmt.getDataverseName(),
                    stmt.getDatasetName());
            List<Integer> keySourceIndicator = ((InternalDatasetDetails) targetDatasource.getDataset()
                    .getDatasetDetails()).getKeySourceIndicator();
            ArrayList<LogicalVariable> vars = new ArrayList<LogicalVariable>();
            ArrayList<Mutable<ILogicalExpression>> exprs = new ArrayList<Mutable<ILogicalExpression>>();
            List<Mutable<ILogicalExpression>> varRefsForLoading = new ArrayList<Mutable<ILogicalExpression>>();
            List<List<String>> partitionKeys = DatasetUtils.getPartitioningKeys(targetDatasource.getDataset());
            int numOfPrimaryKeys = partitionKeys.size();
            for (int i = 0; i < numOfPrimaryKeys; i++) {
                if (keySourceIndicator == null || keySourceIndicator.get(i).intValue() == 0) {
                    // record part
                    PlanTranslationUtil.prepareVarAndExpression(partitionKeys.get(i), resVar, vars, exprs,
                            varRefsForLoading, context);
                } else {
                    // meta part
                    PlanTranslationUtil.prepareMetaKeyAccessExpression(partitionKeys.get(i), unnestVar, exprs, vars,
                            varRefsForLoading, context);
                }
            }

            AssignOperator assign = new AssignOperator(vars, exprs);
            List<String> additionalFilteringField = DatasetUtils.getFilterField(targetDatasource.getDataset());
            List<LogicalVariable> additionalFilteringVars = null;
            List<Mutable<ILogicalExpression>> additionalFilteringAssignExpressions = null;
            List<Mutable<ILogicalExpression>> additionalFilteringExpressions = null;
            AssignOperator additionalFilteringAssign = null;
            if (additionalFilteringField != null) {
                additionalFilteringVars = new ArrayList<LogicalVariable>();
                additionalFilteringAssignExpressions = new ArrayList<Mutable<ILogicalExpression>>();
                additionalFilteringExpressions = new ArrayList<Mutable<ILogicalExpression>>();

                PlanTranslationUtil.prepareVarAndExpression(additionalFilteringField, resVar, additionalFilteringVars,
                        additionalFilteringAssignExpressions, additionalFilteringExpressions, context);

                additionalFilteringAssign = new AssignOperator(additionalFilteringVars,
                        additionalFilteringAssignExpressions);
                additionalFilteringAssign.getInputs().add(new MutableObject<ILogicalOperator>(project));
                assign.getInputs().add(new MutableObject<ILogicalOperator>(additionalFilteringAssign));
            } else {
                assign.getInputs().add(new MutableObject<ILogicalOperator>(project));
            }

            Mutable<ILogicalExpression> varRef = new MutableObject<ILogicalExpression>(
                    new VariableReferenceExpression(resVar));
            ILogicalOperator leafOperator = null;

            switch (stmt.getKind()) {
                case INSERT: {
                    if (targetDatasource.getDataset().hasMetaPart()) {
                        throw new AlgebricksException(targetDatasource.getDataset().getDatasetName()
                                + ": insert into dataset is not supported on Datasets with Meta records");
                    }
                    InsertDeleteUpsertOperator insertOp = new InsertDeleteUpsertOperator(targetDatasource, varRef,
                            varRefsForLoading, InsertDeleteUpsertOperator.Kind.INSERT, false);
                    insertOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
                    insertOp.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                    leafOperator = new SinkOperator();
                    leafOperator.getInputs().add(new MutableObject<ILogicalOperator>(insertOp));
                    break;
                }
                case UPSERT: {
                    if (targetDatasource.getDataset().hasMetaPart()) {
                        throw new AlgebricksException(targetDatasource.getDataset().getDatasetName()
                                + ": upsert into dataset is not supported on Datasets with Meta records");
                    }
                    InsertDeleteUpsertOperator upsertOp = new InsertDeleteUpsertOperator(targetDatasource, varRef,
                            varRefsForLoading, InsertDeleteUpsertOperator.Kind.UPSERT, false);
                    upsertOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
                    upsertOp.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                    // Create and add a new variable used for representing the original record
                    ARecordType recordType = (ARecordType) targetDatasource.getItemType();
                    upsertOp.setPrevRecordVar(context.newVar());
                    upsertOp.setPrevRecordType(recordType);
                    if (additionalFilteringField != null) {
                        upsertOp.setPrevFilterVar(context.newVar());
                        upsertOp.setPrevFilterType(recordType.getFieldType(additionalFilteringField.get(0)));
                    }
                    leafOperator = new SinkOperator();
                    leafOperator.getInputs().add(new MutableObject<ILogicalOperator>(upsertOp));
                    break;
                }
                case DELETE: {
                    if (targetDatasource.getDataset().hasMetaPart()) {
                        throw new AlgebricksException(targetDatasource.getDataset().getDatasetName()
                                + ": delete from dataset is not supported on Datasets with Meta records");
                    }
                    InsertDeleteUpsertOperator deleteOp = new InsertDeleteUpsertOperator(targetDatasource, varRef,
                            varRefsForLoading, InsertDeleteUpsertOperator.Kind.DELETE, false);
                    deleteOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
                    deleteOp.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                    leafOperator = new SinkOperator();
                    leafOperator.getInputs().add(new MutableObject<ILogicalOperator>(deleteOp));
                    break;
                }
                case CONNECT_FEED: {
                    InsertDeleteUpsertOperator insertOp = new InsertDeleteUpsertOperator(targetDatasource, varRef,
                            varRefsForLoading, InsertDeleteUpsertOperator.Kind.INSERT, false);
                    insertOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
                    insertOp.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                    leafOperator = new SinkOperator();
                    leafOperator.getInputs().add(new MutableObject<ILogicalOperator>(insertOp));
                    break;
                }
                case SUBSCRIBE_FEED: {
                    // if the feed is a change feed (i.e, performs different operations), we need to project op variable
                    CompiledSubscribeFeedStatement sfs = (CompiledSubscribeFeedStatement) stmt;
                    InsertDeleteUpsertOperator feedModificationOp;
                    AssignOperator metaAndKeysAssign = null;
                    List<LogicalVariable> metaAndKeysVars = null;
                    List<Mutable<ILogicalExpression>> metaAndKeysExprs = null;
                    List<Mutable<ILogicalExpression>> metaExpSingletonList = null;
                    boolean isChangeFeed = FeedMetadataUtil.isChangeFeed(metadataProvider, sfs.getDataverseName(),
                            sfs.getFeedName());
                    if (targetDatasource.getDataset().hasMetaPart() || isChangeFeed) {
                        metaAndKeysVars = new ArrayList<>();
                        metaAndKeysExprs = new ArrayList<>();
                    }
                    if (targetDatasource.getDataset().hasMetaPart()) {
                        // add the meta function
                        IFunctionInfo finfoMeta = FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.META);
                        ScalarFunctionCallExpression metaFunction = new ScalarFunctionCallExpression(finfoMeta,
                                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(unnestVar)));
                        // create assign for the meta part
                        LogicalVariable metaVar = context.newVar();
                        metaExpSingletonList = new ArrayList<>(1);
                        metaExpSingletonList
                                .add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(metaVar)));
                        metaAndKeysVars.add(metaVar);
                        metaAndKeysExprs.add(new MutableObject<ILogicalExpression>(metaFunction));
                        project.getVariables().add(metaVar);
                    }
                    if (isChangeFeed) {
                        varRefsForLoading.clear();
                        for (Mutable<ILogicalExpression> assignExpr : exprs) {
                            if (assignExpr.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                                AbstractFunctionCallExpression funcCall = (AbstractFunctionCallExpression) assignExpr
                                        .getValue();
                                funcCall.substituteVar(resVar, unnestVar);
                                LogicalVariable pkVar = context.newVar();
                                metaAndKeysVars.add(pkVar);
                                metaAndKeysExprs.add(new MutableObject<ILogicalExpression>(assignExpr.getValue()));
                                project.getVariables().add(pkVar);
                                varRefsForLoading.add(
                                        new MutableObject<ILogicalExpression>(new VariableReferenceExpression(pkVar)));
                            }
                        }
                        // A change feed, we don't need the assign to access PKs
                        feedModificationOp = new InsertDeleteUpsertOperator(targetDatasource, varRef, varRefsForLoading,
                                metaExpSingletonList, InsertDeleteUpsertOperator.Kind.UPSERT, false);
                        // Create and add a new variable used for representing the original record
                        feedModificationOp.setPrevRecordVar(context.newVar());
                        feedModificationOp.setPrevRecordType(targetDatasource.getItemType());
                        if (additionalFilteringField != null) {
                            feedModificationOp.setPrevFilterVar(context.newVar());
                            feedModificationOp.setPrevFilterType(((ARecordType) targetDatasource.getItemType())
                                    .getFieldType(additionalFilteringField.get(0)));
                            additionalFilteringAssign.getInputs().clear();
                            additionalFilteringAssign.getInputs().add(assign.getInputs().get(0));
                            feedModificationOp.getInputs().add(new MutableObject<>(additionalFilteringAssign));
                        } else {
                            feedModificationOp.getInputs().add(assign.getInputs().get(0));
                        }
                    } else {
                        feedModificationOp = new InsertDeleteUpsertOperator(targetDatasource, varRef, varRefsForLoading,
                                metaExpSingletonList, InsertDeleteUpsertOperator.Kind.INSERT, false);
                        feedModificationOp.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                    }
                    if (targetDatasource.getDataset().hasMetaPart() || isChangeFeed) {
                        metaAndKeysAssign = new AssignOperator(metaAndKeysVars, metaAndKeysExprs);
                        metaAndKeysAssign.getInputs().add(project.getInputs().get(0));
                        project.getInputs().set(0, new MutableObject<ILogicalOperator>(metaAndKeysAssign));
                    }
                    feedModificationOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
                    leafOperator = new SinkOperator();
                    leafOperator.getInputs().add(new MutableObject<ILogicalOperator>(feedModificationOp));
                    break;
                }
                default:
                    break;
            }
            topOp = leafOperator;
        }
        globalPlanRoots.add(new MutableObject<ILogicalOperator>(topOp));
        ILogicalPlan plan = new ALogicalPlanImpl(globalPlanRoots);
        eliminateSharedOperatorReferenceForPlan(plan);
        return plan;
    }

    private DatasetDataSource validateDatasetInfo(AqlMetadataProvider metadataProvider, String dataverseName,
            String datasetName) throws AlgebricksException {
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Cannot find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("Cannot write output to an external dataset.");
        }
        AqlSourceId sourceId = new AqlSourceId(dataverseName, datasetName);
        IAType itemType = metadataProvider.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        IAType metaItemType = metadataProvider.findType(dataset.getMetaItemTypeDataverseName(),
                dataset.getMetaItemTypeName());
        INodeDomain domain = metadataProvider.findNodeDomain(dataset.getNodeGroupName());
        DatasetDataSource dataSource = new DatasetDataSource(sourceId, dataset, itemType, metaItemType,
                AqlDataSourceType.INTERNAL_DATASET, dataset.getDatasetDetails(), domain);
        return dataSource;
    }

    private FileSplit getDefaultOutputFileLocation() throws MetadataException {
        String outputDir = System.getProperty("java.io.tmpDir");
        String filePath = outputDir + System.getProperty("file.separator") + OUTPUT_FILE_PREFIX
                + outputFileID.incrementAndGet();
        AsterixMetadataProperties metadataProperties = AsterixAppContextInfo.getInstance().getMetadataProperties();
        return new FileSplit(metadataProperties.getMetadataNodeName(), new FileReference(new File(filePath)));
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(LetClause lc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        LogicalVariable v;
        ILogicalOperator returnedOp;

        switch (lc.getBindingExpr().getKind()) {
            case VARIABLE_EXPRESSION: {
                v = context.newVar(lc.getVarExpr());
                LogicalVariable prev = context.getVar(((VariableExpr) lc.getBindingExpr()).getVar().getId());
                returnedOp = new AssignOperator(v,
                        new MutableObject<ILogicalExpression>(new VariableReferenceExpression(prev)));
                returnedOp.getInputs().add(tupSource);
                break;
            }
            default: {
                v = context.newVar(lc.getVarExpr());
                Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(lc.getBindingExpr(),
                        tupSource);
                returnedOp = new AssignOperator(v, new MutableObject<ILogicalExpression>(eo.first));
                returnedOp.getInputs().add(eo.second);
                break;
            }
        }
        return new Pair<ILogicalOperator, LogicalVariable>(returnedOp, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FieldAccessor fa, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(fa.getExpr(), tupSource);
        LogicalVariable v = context.newVar();
        AbstractFunctionCallExpression fldAccess = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME));
        fldAccess.getArguments().add(new MutableObject<ILogicalExpression>(p.first));
        ILogicalExpression faExpr = new ConstantExpression(
                new AsterixConstantValue(new AString(fa.getIdent().getValue())));
        fldAccess.getArguments().add(new MutableObject<ILogicalExpression>(faExpr));
        AssignOperator a = new AssignOperator(v, new MutableObject<ILogicalExpression>(fldAccess));
        a.getInputs().add(p.second);
        return new Pair<ILogicalOperator, LogicalVariable>(a, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(IndexAccessor ia, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(ia.getExpr(), tupSource);
        LogicalVariable v = context.newVar();
        AbstractFunctionCallExpression f;
        if (ia.isAny()) {
            f = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.ANY_COLLECTION_MEMBER));
            f.getArguments().add(new MutableObject<ILogicalExpression>(p.first));
        } else {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> indexPair = langExprToAlgExpression(ia.getIndexExpr(),
                    tupSource);
            f = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.GET_ITEM));
            f.getArguments().add(new MutableObject<ILogicalExpression>(p.first));
            f.getArguments().add(new MutableObject<ILogicalExpression>(indexPair.first));
        }
        AssignOperator a = new AssignOperator(v, new MutableObject<ILogicalExpression>(f));
        a.getInputs().add(p.second);
        return new Pair<ILogicalOperator, LogicalVariable>(a, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CallExpr fcall, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        LogicalVariable v = context.newVar();
        FunctionSignature signature = fcall.getFunctionSignature();
        List<Mutable<ILogicalExpression>> args = new ArrayList<Mutable<ILogicalExpression>>();
        Mutable<ILogicalOperator> topOp = tupSource;

        for (Expression expr : fcall.getExprList()) {
            switch (expr.getKind()) {
                case VARIABLE_EXPRESSION: {
                    LogicalVariable var = context.getVar(((VariableExpr) expr).getVar().getId());
                    args.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var)));
                    break;
                }
                case LITERAL_EXPRESSION: {
                    LiteralExpr val = (LiteralExpr) expr;
                    args.add(new MutableObject<ILogicalExpression>(new ConstantExpression(
                            new AsterixConstantValue(ConstantHelper.objectFromLiteral(val.getValue())))));
                    break;
                }
                default: {
                    Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(expr, topOp);
                    AbstractLogicalOperator o1 = (AbstractLogicalOperator) eo.second.getValue();
                    args.add(new MutableObject<ILogicalExpression>(eo.first));
                    if (o1 != null && !(o1.getOperatorTag() == LogicalOperatorTag.ASSIGN && hasOnlyChild(o1, topOp))) {
                        topOp = eo.second;
                    }
                    break;
                }
            }
        }

        AbstractFunctionCallExpression f;
        if ((f = lookupUserDefinedFunction(signature, args)) == null) {
            f = lookupBuiltinFunction(signature.getName(), signature.getArity(), args);
        }

        if (f == null) {
            throw new AsterixException(" Unknown function " + signature.getName() + "@" + signature.getArity());
        }

        // Put hints into function call expr.
        if (fcall.hasHints()) {
            for (IExpressionAnnotation hint : fcall.getHints()) {
                f.getAnnotations().put(hint, hint);
            }
        }

        AssignOperator op = new AssignOperator(v, new MutableObject<ILogicalExpression>(f));
        if (topOp != null) {
            op.getInputs().add(topOp);
        }

        return new Pair<ILogicalOperator, LogicalVariable>(op, v);
    }

    private AbstractFunctionCallExpression lookupUserDefinedFunction(FunctionSignature signature,
            List<Mutable<ILogicalExpression>> args) throws MetadataException {
        if (signature.getNamespace() == null) {
            return null;
        }
        Function function = MetadataManager.INSTANCE.getFunction(metadataProvider.getMetadataTxnContext(), signature);
        if (function == null) {
            return null;
        }
        AbstractFunctionCallExpression f = null;
        if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_JAVA)) {
            IFunctionInfo finfo = ExternalFunctionCompilerUtil
                    .getExternalFunctionInfo(metadataProvider.getMetadataTxnContext(), function);
            f = new ScalarFunctionCallExpression(finfo, args);
        } else if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)) {
            IFunctionInfo finfo = FunctionUtil.getFunctionInfo(signature);
            f = new ScalarFunctionCallExpression(finfo, args);
        } else {
            throw new MetadataException(
                    " User defined functions written in " + function.getLanguage() + " are not supported");
        }
        return f;
    }

    private AbstractFunctionCallExpression lookupBuiltinFunction(String functionName, int arity,
            List<Mutable<ILogicalExpression>> args) {
        AbstractFunctionCallExpression f = null;
        FunctionIdentifier fi = new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, functionName, arity);
        AsterixFunctionInfo afi = AsterixBuiltinFunctions.lookupFunction(fi);
        FunctionIdentifier builtinAquafi = afi == null ? null : afi.getFunctionIdentifier();

        if (builtinAquafi != null) {
            fi = builtinAquafi;
        } else {
            fi = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, functionName, arity);
            afi = AsterixBuiltinFunctions.lookupFunction(fi);
            if (afi == null) {
                return null;
            }
        }
        if (AsterixBuiltinFunctions.isBuiltinAggregateFunction(fi)) {
            f = AsterixBuiltinFunctions.makeAggregateFunctionExpression(fi, args);
        } else if (AsterixBuiltinFunctions.isBuiltinUnnestingFunction(fi)) {
            UnnestingFunctionCallExpression ufce = new UnnestingFunctionCallExpression(FunctionUtil.getFunctionInfo(fi),
                    args);
            ufce.setReturnsUniqueValues(AsterixBuiltinFunctions.returnsUniqueValues(fi));
            f = ufce;
        } else {
            f = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(fi), args);
        }
        return f;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FunctionDecl fd, Mutable<ILogicalOperator> tupSource) {
        throw new IllegalStateException("Function declarations should be inlined at AST rewriting phase.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(GroupbyClause gc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Mutable<ILogicalOperator> topOp = tupSource;
        if (gc.hasGroupVar()) {
            List<Pair<Expression, Identifier>> groupFieldList = gc.getGroupFieldList();
            List<Mutable<ILogicalExpression>> groupRecordConstructorArgList = new ArrayList<>();
            for (Pair<Expression, Identifier> groupField : groupFieldList) {
                ILogicalExpression groupFieldNameExpr = langExprToAlgExpression(
                        new LiteralExpr(new StringLiteral(groupField.second.getValue())), topOp).first;
                groupRecordConstructorArgList.add(new MutableObject<ILogicalExpression>(groupFieldNameExpr));
                ILogicalExpression groupFieldExpr = langExprToAlgExpression(groupField.first, topOp).first;
                groupRecordConstructorArgList.add(new MutableObject<ILogicalExpression>(groupFieldExpr));
            }
            LogicalVariable groupVar = context.newVar(gc.getGroupVar());
            AssignOperator groupVarAssignOp = new AssignOperator(groupVar,
                    new MutableObject<ILogicalExpression>(new ScalarFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR),
                            groupRecordConstructorArgList)));
            groupVarAssignOp.getInputs().add(topOp);
            topOp = new MutableObject<ILogicalOperator>(groupVarAssignOp);
        }
        if (gc.isGroupAll()) {
            List<LogicalVariable> aggVars = new ArrayList<>();
            List<Mutable<ILogicalExpression>> aggFuncs = new ArrayList<>();
            for (VariableExpr var : gc.getWithVarList()) {
                LogicalVariable aggVar = context.newVar();
                LogicalVariable oldVar = context.getVar(var);
                List<Mutable<ILogicalExpression>> flArgs = new ArrayList<Mutable<ILogicalExpression>>();
                flArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(oldVar)));
                AggregateFunctionCallExpression fListify = AsterixBuiltinFunctions
                        .makeAggregateFunctionExpression(AsterixBuiltinFunctions.LISTIFY, flArgs);
                aggVars.add(aggVar);
                aggFuncs.add(new MutableObject<ILogicalExpression>(fListify));
                // Hide the variable that was part of the "with", replacing it with
                // the one bound by the aggregation op.
                context.setVar(var, aggVar);
            }
            AggregateOperator aggOp = new AggregateOperator(aggVars, aggFuncs);
            aggOp.getInputs().add(topOp);
            return new Pair<ILogicalOperator, LogicalVariable>(aggOp, null);
        } else {
            GroupByOperator gOp = new GroupByOperator();
            for (GbyVariableExpressionPair ve : gc.getGbyPairList()) {
                LogicalVariable v;
                VariableExpr vexpr = ve.getVar();
                if (vexpr != null) {
                    v = context.newVar(vexpr);
                } else {
                    v = context.newVar();
                }
                Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(ve.getExpr(), topOp);
                gOp.addGbyExpression(v, eo.first);
                topOp = eo.second;
            }
            for (GbyVariableExpressionPair ve : gc.getDecorPairList()) {
                LogicalVariable v;
                VariableExpr vexpr = ve.getVar();
                if (vexpr != null) {
                    v = context.newVar(vexpr);
                } else {
                    v = context.newVar();
                }
                Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(ve.getExpr(), topOp);
                gOp.addDecorExpression(v, eo.first);
                topOp = eo.second;
            }

            gOp.getInputs().add(topOp);
            for (VariableExpr var : gc.getWithVarList()) {
                LogicalVariable aggVar = context.newVar();
                LogicalVariable oldVar = context.getVar(var);
                List<Mutable<ILogicalExpression>> flArgs = new ArrayList<Mutable<ILogicalExpression>>(1);
                flArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(oldVar)));
                AggregateFunctionCallExpression fListify = AsterixBuiltinFunctions
                        .makeAggregateFunctionExpression(AsterixBuiltinFunctions.LISTIFY, flArgs);
                AggregateOperator agg = new AggregateOperator(mkSingletonArrayList(aggVar),
                        (List) mkSingletonArrayList(new MutableObject<ILogicalExpression>(fListify)));

                agg.getInputs().add(new MutableObject<ILogicalOperator>(
                        new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(gOp))));
                ILogicalPlan plan = new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(agg));
                gOp.getNestedPlans().add(plan);
                // Hide the variable that was part of the "with", replacing it with
                // the one bound by the aggregation op.
                context.setVar(var, aggVar);
            }
            gOp.getAnnotations().put(OperatorAnnotations.USE_HASH_GROUP_BY, gc.hasHashGroupByHint());
            return new Pair<ILogicalOperator, LogicalVariable>(gOp, null);
        }

    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(IfExpr ifexpr, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        // In the most general case, IfThenElse is translated in the following
        // way.
        //
        // We assign the result of the condition to one variable varCond.
        // We create one subplan which contains the plan for the "then" branch,
        // on top of which there is a selection whose condition is varCond.
        // Similarly, we create one subplan for the "else" branch, in which the
        // selection is not(varCond).
        // Finally, we select the desired result.
        Pair<ILogicalOperator, LogicalVariable> pCond = ifexpr.getCondExpr().accept(this, tupSource);
        LogicalVariable varCond = pCond.second;

        //Creates a subplan for the "then" branch.
        Pair<ILogicalOperator, LogicalVariable> opAndVarForThen = constructSubplanOperatorForBranch(pCond.first,
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(varCond)), ifexpr.getThenExpr());

        // Creates a subplan for the "else" branch.
        AbstractFunctionCallExpression notVarCond = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.NOT), Collections.singletonList(
                        new MutableObject<ILogicalExpression>(new VariableReferenceExpression(varCond))));
        Pair<ILogicalOperator, LogicalVariable> opAndVarForElse = constructSubplanOperatorForBranch(
                opAndVarForThen.first, new MutableObject<ILogicalExpression>(notVarCond), ifexpr.getElseExpr());

        // Uses switch-case function to select the results of two branches.
        LogicalVariable selectVar = context.newVar();
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<>();
        arguments.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(varCond)));
        arguments.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        arguments.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(opAndVarForThen.second)));
        arguments.add(new MutableObject<ILogicalExpression>(ConstantExpression.FALSE));
        arguments.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(opAndVarForElse.second)));
        AbstractFunctionCallExpression swithCaseExpr = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.SWITCH_CASE), arguments);
        AssignOperator assignOp = new AssignOperator(selectVar, new MutableObject<ILogicalExpression>(swithCaseExpr));
        assignOp.getInputs().add(new MutableObject<ILogicalOperator>(opAndVarForElse.first));

        // Unnests the selected ("if" or "else") result.
        LogicalVariable unnestVar = context.newVar();
        UnnestOperator unnestOp = new UnnestOperator(unnestVar,
                new MutableObject<ILogicalExpression>(new UnnestingFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION),
                        Collections.singletonList(
                                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(selectVar))))));
        unnestOp.getInputs().add(new MutableObject<ILogicalOperator>(assignOp));

        // Produces the final result.
        LogicalVariable resultVar = context.newVar();
        AssignOperator finalAssignOp = new AssignOperator(resultVar,
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(unnestVar)));
        finalAssignOp.getInputs().add(new MutableObject<ILogicalOperator>(unnestOp));
        return new Pair<ILogicalOperator, LogicalVariable>(finalAssignOp, resultVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(LiteralExpr l, Mutable<ILogicalOperator> tupSource) {
        LogicalVariable var = context.newVar();
        AssignOperator a = new AssignOperator(var, new MutableObject<ILogicalExpression>(
                new ConstantExpression(new AsterixConstantValue(ConstantHelper.objectFromLiteral(l.getValue())))));
        if (tupSource != null) {
            a.getInputs().add(tupSource);
        }
        return new Pair<ILogicalOperator, LogicalVariable>(a, var);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(OperatorExpr op, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        List<OperatorType> ops = op.getOpList();
        int nOps = ops.size();

        if (nOps > 0 && (ops.get(0) == OperatorType.AND || ops.get(0) == OperatorType.OR)) {
            return visitAndOrOperator(op, tupSource);
        }

        List<Expression> exprs = op.getExprList();

        Mutable<ILogicalOperator> topOp = tupSource;

        ILogicalExpression currExpr = null;
        for (int i = 0; i <= nOps; i++) {

            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(exprs.get(i), topOp);
            topOp = p.second;
            ILogicalExpression e = p.first;
            // now look at the operator
            if (i < nOps) {
                if (OperatorExpr.opIsComparison(ops.get(i))) {
                    AbstractFunctionCallExpression c = createComparisonExpression(ops.get(i));

                    // chain the operators
                    if (i == 0) {
                        c.getArguments().add(new MutableObject<ILogicalExpression>(e));
                        currExpr = c;
                        if (op.isBroadcastOperand(i)) {
                            BroadcastExpressionAnnotation bcast = new BroadcastExpressionAnnotation();
                            bcast.setObject(BroadcastSide.LEFT);
                            c.getAnnotations().put(BroadcastExpressionAnnotation.BROADCAST_ANNOTATION_KEY, bcast);
                        }
                    } else {
                        ((AbstractFunctionCallExpression) currExpr).getArguments()
                                .add(new MutableObject<ILogicalExpression>(e));
                        c.getArguments().add(new MutableObject<ILogicalExpression>(currExpr));
                        currExpr = c;
                        if (i == 1 && op.isBroadcastOperand(i)) {
                            BroadcastExpressionAnnotation bcast = new BroadcastExpressionAnnotation();
                            bcast.setObject(BroadcastSide.RIGHT);
                            c.getAnnotations().put(BroadcastExpressionAnnotation.BROADCAST_ANNOTATION_KEY, bcast);
                        }
                    }
                } else {
                    AbstractFunctionCallExpression f = createFunctionCallExpressionForBuiltinOperator(ops.get(i));

                    if (i == 0) {
                        f.getArguments().add(new MutableObject<ILogicalExpression>(e));
                        currExpr = f;
                    } else {
                        ((AbstractFunctionCallExpression) currExpr).getArguments()
                                .add(new MutableObject<ILogicalExpression>(e));
                        f.getArguments().add(new MutableObject<ILogicalExpression>(currExpr));
                        currExpr = f;
                    }
                }
            } else { // don't forget the last expression...
                ((AbstractFunctionCallExpression) currExpr).getArguments()
                        .add(new MutableObject<ILogicalExpression>(e));
                if (i == 1 && op.isBroadcastOperand(i)) {
                    BroadcastExpressionAnnotation bcast = new BroadcastExpressionAnnotation();
                    bcast.setObject(BroadcastSide.RIGHT);
                    ((AbstractFunctionCallExpression) currExpr).getAnnotations()
                            .put(BroadcastExpressionAnnotation.BROADCAST_ANNOTATION_KEY, bcast);
                }
            }
        }

        // Add hints as annotations.
        if (op.hasHints() && (currExpr instanceof AbstractFunctionCallExpression)) {
            AbstractFunctionCallExpression currFuncExpr = (AbstractFunctionCallExpression) currExpr;
            for (IExpressionAnnotation hint : op.getHints()) {
                currFuncExpr.getAnnotations().put(hint, hint);
            }
        }

        LogicalVariable assignedVar = context.newVar();
        AssignOperator a = new AssignOperator(assignedVar, new MutableObject<ILogicalExpression>(currExpr));

        a.getInputs().add(topOp);

        return new Pair<ILogicalOperator, LogicalVariable>(a, assignedVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(OrderbyClause oc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        OrderOperator ord = new OrderOperator();
        Iterator<OrderModifier> modifIter = oc.getModifierList().iterator();
        Mutable<ILogicalOperator> topOp = tupSource;
        for (Expression e : oc.getOrderbyList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(e, topOp);
            OrderModifier m = modifIter.next();
            OrderOperator.IOrder comp = (m == OrderModifier.ASC) ? OrderOperator.ASC_ORDER : OrderOperator.DESC_ORDER;
            ord.getOrderExpressions().add(new Pair<IOrder, Mutable<ILogicalExpression>>(comp,
                    new MutableObject<ILogicalExpression>(p.first)));
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
            boolean ascending = (orderModifIter.next() == OrderModifier.ASC);
            RangeMapBuilder.verifyRangeOrder(oc.getRangeMap(), ascending);
            ord.getAnnotations().put(OperatorAnnotations.USE_RANGE_CONNECTOR, oc.getRangeMap());
        }
        return new Pair<ILogicalOperator, LogicalVariable>(ord, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(QuantifiedExpression qe, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Mutable<ILogicalOperator> topOp = tupSource;

        ILogicalOperator firstOp = null;
        Mutable<ILogicalOperator> lastOp = null;

        for (QuantifiedPair qt : qe.getQuantifiedList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo1 = langExprToAlgExpression(qt.getExpr(), topOp);
            topOp = eo1.second;
            LogicalVariable uVar = context.newVar(qt.getVarExpr());
            ILogicalOperator u = new UnnestOperator(uVar,
                    new MutableObject<ILogicalExpression>(makeUnnestExpression(eo1.first)));

            if (firstOp == null) {
                firstOp = u;
            }
            if (lastOp != null) {
                u.getInputs().add(lastOp);
            }
            lastOp = new MutableObject<ILogicalOperator>(u);
        }

        // We make all the unnest correspond. to quantif. vars. sit on top
        // in the hope of enabling joins & other optimiz.
        firstOp.getInputs().add(topOp);
        topOp = lastOp;

        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo2 = langExprToAlgExpression(qe.getSatisfiesExpr(), topOp);

        AggregateFunctionCallExpression fAgg;
        SelectOperator s;
        if (qe.getQuantifier() == Quantifier.SOME) {
            s = new SelectOperator(new MutableObject<ILogicalExpression>(eo2.first), false, null);
            s.getInputs().add(eo2.second);
            fAgg = AsterixBuiltinFunctions.makeAggregateFunctionExpression(AsterixBuiltinFunctions.NON_EMPTY_STREAM,
                    new ArrayList<Mutable<ILogicalExpression>>());
        } else { // EVERY
            List<Mutable<ILogicalExpression>> satExprList = new ArrayList<Mutable<ILogicalExpression>>(1);
            satExprList.add(new MutableObject<ILogicalExpression>(eo2.first));
            s = new SelectOperator(new MutableObject<ILogicalExpression>(new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.NOT), satExprList)), false, null);
            s.getInputs().add(eo2.second);
            fAgg = AsterixBuiltinFunctions.makeAggregateFunctionExpression(AsterixBuiltinFunctions.EMPTY_STREAM,
                    new ArrayList<Mutable<ILogicalExpression>>());
        }
        LogicalVariable qeVar = context.newVar();
        AggregateOperator a = new AggregateOperator(mkSingletonArrayList(qeVar),
                (List) mkSingletonArrayList(new MutableObject<ILogicalExpression>(fAgg)));
        a.getInputs().add(new MutableObject<ILogicalOperator>(s));
        return new Pair<ILogicalOperator, LogicalVariable>(a, qeVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(Query q, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        return q.getBody().accept(this, tupSource);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(RecordConstructor rc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        AbstractFunctionCallExpression f = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR));
        LogicalVariable v1 = context.newVar();
        AssignOperator a = new AssignOperator(v1, new MutableObject<ILogicalExpression>(f));
        Mutable<ILogicalOperator> topOp = tupSource;
        for (FieldBinding fb : rc.getFbList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo1 = langExprToAlgExpression(fb.getLeftExpr(), topOp);
            f.getArguments().add(new MutableObject<ILogicalExpression>(eo1.first));
            topOp = eo1.second;
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo2 = langExprToAlgExpression(fb.getRightExpr(), topOp);
            f.getArguments().add(new MutableObject<ILogicalExpression>(eo2.first));
            topOp = eo2.second;
        }
        a.getInputs().add(topOp);
        return new Pair<ILogicalOperator, LogicalVariable>(a, v1);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(ListConstructor lc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        FunctionIdentifier fid = (lc.getType() == Type.ORDERED_LIST_CONSTRUCTOR)
                ? AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR : AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR;
        AbstractFunctionCallExpression f = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(fid));
        LogicalVariable v1 = context.newVar();
        AssignOperator a = new AssignOperator(v1, new MutableObject<ILogicalExpression>(f));
        Mutable<ILogicalOperator> topOp = tupSource;
        for (Expression expr : lc.getExprList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(expr, topOp);
            f.getArguments().add(new MutableObject<ILogicalExpression>(eo.first));
            topOp = eo.second;
        }
        a.getInputs().add(topOp);
        return new Pair<ILogicalOperator, LogicalVariable>(a, v1);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(UnaryExpr u, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Expression expr = u.getExpr();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(expr, tupSource);
        LogicalVariable v1 = context.newVar();
        AssignOperator a;
        if (u.getSign() == Sign.POSITIVE) {
            a = new AssignOperator(v1, new MutableObject<ILogicalExpression>(eo.first));
        } else {
            AbstractFunctionCallExpression m = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.NUMERIC_UNARY_MINUS));
            m.getArguments().add(new MutableObject<ILogicalExpression>(eo.first));
            a = new AssignOperator(v1, new MutableObject<ILogicalExpression>(m));
        }
        a.getInputs().add(eo.second);
        return new Pair<ILogicalOperator, LogicalVariable>(a, v1);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(VariableExpr v, Mutable<ILogicalOperator> tupSource) {
        // Should we ever get to this method?
        LogicalVariable var = context.newVar();
        LogicalVariable oldV = context.getVar(v.getVar().getId());
        AssignOperator a = new AssignOperator(var,
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(oldV)));
        a.getInputs().add(tupSource);
        return new Pair<ILogicalOperator, LogicalVariable>(a, var);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(WhereClause w, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(w.getWhereExpr(), tupSource);
        SelectOperator s = new SelectOperator(new MutableObject<ILogicalExpression>(p.first), false, null);
        s.getInputs().add(p.second);
        return new Pair<ILogicalOperator, LogicalVariable>(s, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(LimitClause lc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p1 = langExprToAlgExpression(lc.getLimitExpr(), tupSource);
        LimitOperator opLim;
        Expression offset = lc.getOffset();
        if (offset != null) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p2 = langExprToAlgExpression(offset, p1.second);
            opLim = new LimitOperator(p1.first, p2.first);
            opLim.getInputs().add(p2.second);
        } else {
            opLim = new LimitOperator(p1.first);
            opLim.getInputs().add(p1.second);
        }
        return new Pair<ILogicalOperator, LogicalVariable>(opLim, null);
    }

    protected AbstractFunctionCallExpression createComparisonExpression(OperatorType t) {
        FunctionIdentifier fi = operatorTypeToFunctionIdentifier(t);
        IFunctionInfo finfo = FunctionUtil.getFunctionInfo(fi);
        return new ScalarFunctionCallExpression(finfo);
    }

    private FunctionIdentifier operatorTypeToFunctionIdentifier(OperatorType t) {
        switch (t) {
            case EQ: {
                return AlgebricksBuiltinFunctions.EQ;
            }
            case NEQ: {
                return AlgebricksBuiltinFunctions.NEQ;
            }
            case GT: {
                return AlgebricksBuiltinFunctions.GT;
            }
            case GE: {
                return AlgebricksBuiltinFunctions.GE;
            }
            case LT: {
                return AlgebricksBuiltinFunctions.LT;
            }
            case LE: {
                return AlgebricksBuiltinFunctions.LE;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    protected AbstractFunctionCallExpression createFunctionCallExpressionForBuiltinOperator(OperatorType t)
            throws AsterixException {

        FunctionIdentifier fid = null;
        switch (t) {
            case PLUS: {
                fid = AlgebricksBuiltinFunctions.NUMERIC_ADD;
                break;
            }
            case MINUS: {
                fid = AsterixBuiltinFunctions.NUMERIC_SUBTRACT;
                break;
            }
            case MUL: {
                fid = AsterixBuiltinFunctions.NUMERIC_MULTIPLY;
                break;
            }
            case DIV: {
                fid = AsterixBuiltinFunctions.NUMERIC_DIVIDE;
                break;
            }
            case MOD: {
                fid = AsterixBuiltinFunctions.NUMERIC_MOD;
                break;
            }
            case IDIV: {
                fid = AsterixBuiltinFunctions.NUMERIC_IDIV;
                break;
            }
            case CARET: {
                fid = AsterixBuiltinFunctions.CARET;
                break;
            }
            case AND: {
                fid = AlgebricksBuiltinFunctions.AND;
                break;
            }
            case OR: {
                fid = AlgebricksBuiltinFunctions.OR;
                break;
            }
            case FUZZY_EQ: {
                fid = AsterixBuiltinFunctions.FUZZY_EQ;
                break;
            }

            default: {
                throw new NotImplementedException("Operator " + t + " is not yet implemented");
            }
        }
        return new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(fid));
    }

    private static boolean hasOnlyChild(ILogicalOperator parent, Mutable<ILogicalOperator> childCandidate) {
        List<Mutable<ILogicalOperator>> inp = parent.getInputs();
        if (inp == null || inp.size() != 1) {
            return false;
        }
        return inp.get(0) == childCandidate;
    }

    protected Pair<ILogicalExpression, Mutable<ILogicalOperator>> langExprToAlgExpression(Expression expr,
            Mutable<ILogicalOperator> topOpRef) throws AsterixException {
        switch (expr.getKind()) {
            case VARIABLE_EXPRESSION: {
                VariableReferenceExpression ve = new VariableReferenceExpression(
                        context.getVar(((VariableExpr) expr).getVar().getId()));
                return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(ve, topOpRef);
            }
            case LITERAL_EXPRESSION: {
                LiteralExpr val = (LiteralExpr) expr;
                return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(new ConstantExpression(
                        new AsterixConstantValue(ConstantHelper.objectFromLiteral(val.getValue()))), topOpRef);
            }
            default: {
                if (expressionNeedsNoNesting(expr)) {
                    Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this, topOpRef);
                    ILogicalExpression exp = ((AssignOperator) p.first).getExpressions().get(0).getValue();
                    return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(exp, p.first.getInputs().get(0));
                } else {
                    Mutable<ILogicalOperator> srcRef = new MutableObject<ILogicalOperator>();
                    Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this, srcRef);
                    if (p.first.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                        if (topOpRef.getValue() != null) {
                            srcRef.setValue(topOpRef.getValue());
                        } else {
                            // Re-binds the bottom operator reference to {@code topOpRef}.
                            rebindBottomOpRef(p.first, srcRef, topOpRef);
                        }
                        Mutable<ILogicalOperator> top2 = new MutableObject<ILogicalOperator>(p.first);
                        return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(
                                new VariableReferenceExpression(p.second), top2);
                    } else {
                        SubplanOperator s = new SubplanOperator();
                        s.getInputs().add(topOpRef);
                        srcRef.setValue(new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(s)));
                        Mutable<ILogicalOperator> planRoot = new MutableObject<ILogicalOperator>(p.first);
                        s.setRootOp(planRoot);
                        return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(
                                new VariableReferenceExpression(p.second), new MutableObject<ILogicalOperator>(s));
                    }
                }
            }
        }
    }

    protected Pair<ILogicalOperator, LogicalVariable> aggListifyForSubquery(LogicalVariable var,
            Mutable<ILogicalOperator> opRef, boolean bProject) {
        AggregateFunctionCallExpression funAgg = AsterixBuiltinFunctions.makeAggregateFunctionExpression(
                AsterixBuiltinFunctions.LISTIFY, new ArrayList<Mutable<ILogicalExpression>>());
        funAgg.getArguments().add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var)));

        LogicalVariable varListified = context.newSubplanOutputVar();
        AggregateOperator agg = new AggregateOperator(mkSingletonArrayList(varListified),
                (List) mkSingletonArrayList(new MutableObject<ILogicalExpression>(funAgg)));
        agg.getInputs().add(opRef);
        ILogicalOperator res;
        if (bProject) {
            ProjectOperator pr = new ProjectOperator(varListified);
            pr.getInputs().add(new MutableObject<ILogicalOperator>(agg));
            res = pr;
        } else {
            res = agg;
        }
        return new Pair<ILogicalOperator, LogicalVariable>(res, varListified);
    }

    protected Pair<ILogicalOperator, LogicalVariable> visitAndOrOperator(OperatorExpr op,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        List<OperatorType> ops = op.getOpList();
        int nOps = ops.size();

        List<Expression> exprs = op.getExprList();

        Mutable<ILogicalOperator> topOp = tupSource;

        OperatorType opLogical = ops.get(0);
        AbstractFunctionCallExpression f = createFunctionCallExpressionForBuiltinOperator(opLogical);

        for (int i = 0; i <= nOps; i++) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(exprs.get(i), topOp);
            topOp = p.second;
            // now look at the operator
            if (i < nOps) {
                if (ops.get(i) != opLogical) {
                    throw new TranslationException(
                            "Unexpected operator " + ops.get(i) + " in an OperatorExpr starting with " + opLogical);
                }
            }
            f.getArguments().add(new MutableObject<ILogicalExpression>(p.first));
        }

        LogicalVariable assignedVar = context.newVar();
        AssignOperator a = new AssignOperator(assignedVar, new MutableObject<ILogicalExpression>(f));
        a.getInputs().add(topOp);

        return new Pair<ILogicalOperator, LogicalVariable>(a, assignedVar);

    }

    protected boolean expressionNeedsNoNesting(Expression expr) {
        Kind k = expr.getKind();
        return (k == Kind.LITERAL_EXPRESSION) || (k == Kind.LIST_CONSTRUCTOR_EXPRESSION)
                || (k == Kind.RECORD_CONSTRUCTOR_EXPRESSION) || (k == Kind.VARIABLE_EXPRESSION)
                || (k == Kind.CALL_EXPRESSION) || (k == Kind.OP_EXPRESSION) || (k == Kind.FIELD_ACCESSOR_EXPRESSION)
                || (k == Kind.INDEX_ACCESSOR_EXPRESSION) || (k == Kind.UNARY_EXPRESSION) || (k == Kind.IF_EXPRESSION);
    }

    protected <T> List<T> mkSingletonArrayList(T item) {
        ArrayList<T> array = new ArrayList<T>(1);
        array.add(item);
        return array;
    }

    protected ILogicalExpression makeUnnestExpression(ILogicalExpression expr) {
        List<Mutable<ILogicalExpression>> argRefs = new ArrayList<>();
        argRefs.add(new MutableObject<ILogicalExpression>(expr));
        switch (expr.getExpressionTag()) {
            case VARIABLE: {
                return new UnnestingFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION), argRefs);
            }
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                if (fce.getKind() == FunctionKind.UNNEST) {
                    return expr;
                } else {
                    return new UnnestingFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION), argRefs);
                }
            }
            default: {
                return expr;
            }
        }
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
     * Eliminate shared operator references in a query plan.
     * Deep copy a new query plan subtree whenever there is a shared operator reference.
     *
     * @param plan,
     *            the query plan.
     * @throws AsterixException
     */
    private void eliminateSharedOperatorReferenceForPlan(ILogicalPlan plan) throws AsterixException {
        for (Mutable<ILogicalOperator> opRef : plan.getRoots()) {
            Set<Mutable<ILogicalOperator>> opRefSet = new HashSet<>();
            eliminateSharedOperatorReference(opRef, opRefSet);
        }
    }

    /**
     * Eliminate shared operator references in a query plan rooted at <code>currentOpRef.getValue()</code>.
     * Deep copy a new query plan subtree whenever there is a shared operator reference.
     *
     * @param currentOpRef,
     *            the operator reference to consider
     * @param opRefSet,
     *            the set storing seen operator references so far.
     * @return a mapping that maps old variables to new variables, for the ancestors of
     *         <code>currentOpRef</code> to replace variables properly.
     * @throws AsterixException
     */
    private Map<LogicalVariable, LogicalVariable> eliminateSharedOperatorReference(
            Mutable<ILogicalOperator> currentOpRef, Set<Mutable<ILogicalOperator>> opRefSet) throws AsterixException {
        try {
            opRefSet.add(currentOpRef);
            AbstractLogicalOperator currentOperator = (AbstractLogicalOperator) currentOpRef.getValue();

            // Recursively eliminates shared references in nested plans.
            if (currentOperator.hasNestedPlans()) {
                // Since a nested plan tree itself can never be shared with another nested plan tree in
                // another operator, the operation called in the if block does not need to replace
                // any variables further for <code>currentOpRef.getValue()</code> nor its ancestor.
                AbstractOperatorWithNestedPlans opWithNestedPlan = (AbstractOperatorWithNestedPlans) currentOperator;
                for (ILogicalPlan plan : opWithNestedPlan.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> rootRef : plan.getRoots()) {
                        Set<Mutable<ILogicalOperator>> nestedOpRefSet = new HashSet<>();
                        eliminateSharedOperatorReference(rootRef, nestedOpRefSet);
                    }
                }
            }

            int childIndex = 0;
            Map<LogicalVariable, LogicalVariable> varMap = new HashMap<>();
            for (Mutable<ILogicalOperator> childRef : currentOperator.getInputs()) {
                if (opRefSet.contains(childRef)) {
                    // There is a shared operator reference in the query plan.
                    // Deep copies the child plan.
                    LogicalOperatorDeepCopyWithNewVariablesVisitor visitor = new LogicalOperatorDeepCopyWithNewVariablesVisitor(
                            context, null);
                    ILogicalOperator newChild = childRef.getValue().accept(visitor, null);
                    Map<LogicalVariable, LogicalVariable> cloneVarMap = visitor.getInputToOutputVariableMapping();

                    // Substitute variables according to the deep copy which generates new variables.
                    VariableUtilities.substituteVariables(currentOperator, cloneVarMap, null);
                    varMap.putAll(cloneVarMap);

                    // Sets the new child.
                    childRef = new MutableObject<ILogicalOperator>(newChild);
                    currentOperator.getInputs().set(childIndex, childRef);
                }

                // Recursively eliminate shared operator reference for the operator subtree,
                // even if it is a deep copy of some other one.
                Map<LogicalVariable, LogicalVariable> childVarMap = eliminateSharedOperatorReference(childRef,
                        opRefSet);
                // Substitute variables according to the new subtree.
                VariableUtilities.substituteVariables(currentOperator, childVarMap, null);

                // Updates mapping like <$a, $b> in varMap to <$a, $c>, where there is a mapping <$b, $c>
                // in childVarMap.
                for (Map.Entry<LogicalVariable, LogicalVariable> entry : varMap.entrySet()) {
                    LogicalVariable newVar = childVarMap.get(entry.getValue());
                    if (newVar != null) {
                        entry.setValue(newVar);
                    }
                }
                varMap.putAll(childVarMap);
                ++childIndex;
            }

            // Only retain live variables for parent operators to substitute variables.
            Set<LogicalVariable> liveVars = new HashSet<>();
            VariableUtilities.getLiveVariables(currentOperator, liveVars);
            varMap.values().retainAll(liveVars);
            return varMap;
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
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
     * @return a pair of the constructed subplan operator and the output variable for the branch.
     * @throws AsterixException
     */
    private Pair<ILogicalOperator, LogicalVariable> constructSubplanOperatorForBranch(ILogicalOperator inputOp,
            Mutable<ILogicalExpression> selectExpr, Expression branchExpression) throws AsterixException {
        context.enterSubplan();
        SubplanOperator subplanOp = new SubplanOperator();
        subplanOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        Mutable<ILogicalOperator> nestedSource = new MutableObject<ILogicalOperator>(
                new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(subplanOp)));
        SelectOperator select = new SelectOperator(selectExpr, false, null);
        // The select operator cannot be moved up and down, otherwise it will cause typing issues (ASTERIXDB-1203).
        OperatorPropertiesUtil.markMovable(select, false);
        select.getInputs().add(nestedSource);
        Pair<ILogicalOperator, LogicalVariable> pBranch = branchExpression.accept(this,
                new MutableObject<ILogicalOperator>(select));
        LogicalVariable branchVar = context.newVar();
        AggregateOperator aggOp = new AggregateOperator(Collections.singletonList(branchVar),
                Collections.singletonList(new MutableObject<ILogicalExpression>(new AggregateFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.LISTIFY), false,
                        Collections.singletonList(new MutableObject<ILogicalExpression>(
                                new VariableReferenceExpression(pBranch.second)))))));
        aggOp.getInputs().add(new MutableObject<ILogicalOperator>(pBranch.first));
        ILogicalPlan planForBranch = new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(aggOp));
        subplanOp.getNestedPlans().add(planForBranch);
        context.exitSubplan();
        return new Pair<ILogicalOperator, LogicalVariable>(subplanOp, branchVar);
    }
}
