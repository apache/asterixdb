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
package edu.uci.ics.asterix.translator;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Expression.Kind;
import edu.uci.ics.asterix.aql.expression.BeginFeedStatement;
import edu.uci.ics.asterix.aql.expression.CallExpr;
import edu.uci.ics.asterix.aql.expression.CompactStatement;
import edu.uci.ics.asterix.aql.expression.ControlFeedStatement;
import edu.uci.ics.asterix.aql.expression.CreateDataverseStatement;
import edu.uci.ics.asterix.aql.expression.CreateFunctionStatement;
import edu.uci.ics.asterix.aql.expression.CreateIndexStatement;
import edu.uci.ics.asterix.aql.expression.DatasetDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDropStatement;
import edu.uci.ics.asterix.aql.expression.DeleteStatement;
import edu.uci.ics.asterix.aql.expression.DistinctClause;
import edu.uci.ics.asterix.aql.expression.DropStatement;
import edu.uci.ics.asterix.aql.expression.FLWOGRExpression;
import edu.uci.ics.asterix.aql.expression.FieldAccessor;
import edu.uci.ics.asterix.aql.expression.FieldBinding;
import edu.uci.ics.asterix.aql.expression.ForClause;
import edu.uci.ics.asterix.aql.expression.FunctionDecl;
import edu.uci.ics.asterix.aql.expression.FunctionDropStatement;
import edu.uci.ics.asterix.aql.expression.GbyVariableExpressionPair;
import edu.uci.ics.asterix.aql.expression.GroupbyClause;
import edu.uci.ics.asterix.aql.expression.IfExpr;
import edu.uci.ics.asterix.aql.expression.IndexAccessor;
import edu.uci.ics.asterix.aql.expression.IndexDropStatement;
import edu.uci.ics.asterix.aql.expression.InsertStatement;
import edu.uci.ics.asterix.aql.expression.LetClause;
import edu.uci.ics.asterix.aql.expression.LimitClause;
import edu.uci.ics.asterix.aql.expression.ListConstructor;
import edu.uci.ics.asterix.aql.expression.ListConstructor.Type;
import edu.uci.ics.asterix.aql.expression.LiteralExpr;
import edu.uci.ics.asterix.aql.expression.LoadFromFileStatement;
import edu.uci.ics.asterix.aql.expression.NodeGroupDropStatement;
import edu.uci.ics.asterix.aql.expression.NodegroupDecl;
import edu.uci.ics.asterix.aql.expression.OperatorExpr;
import edu.uci.ics.asterix.aql.expression.OperatorType;
import edu.uci.ics.asterix.aql.expression.OrderbyClause;
import edu.uci.ics.asterix.aql.expression.OrderbyClause.OrderModifier;
import edu.uci.ics.asterix.aql.expression.OrderedListTypeDefinition;
import edu.uci.ics.asterix.aql.expression.QuantifiedExpression;
import edu.uci.ics.asterix.aql.expression.QuantifiedExpression.Quantifier;
import edu.uci.ics.asterix.aql.expression.QuantifiedPair;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.RecordConstructor;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition;
import edu.uci.ics.asterix.aql.expression.SetStatement;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.TypeDropStatement;
import edu.uci.ics.asterix.aql.expression.TypeReferenceExpression;
import edu.uci.ics.asterix.aql.expression.UnaryExpr;
import edu.uci.ics.asterix.aql.expression.UnaryExpr.Sign;
import edu.uci.ics.asterix.aql.expression.UnionExpr;
import edu.uci.ics.asterix.aql.expression.UnorderedListTypeDefinition;
import edu.uci.ics.asterix.aql.expression.UpdateClause;
import edu.uci.ics.asterix.aql.expression.UpdateStatement;
import edu.uci.ics.asterix.aql.expression.VariableExpr;
import edu.uci.ics.asterix.aql.expression.WhereClause;
import edu.uci.ics.asterix.aql.expression.WriteStatement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.AsterixMetadataProperties;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.metadata.declared.ResultSetDataSink;
import edu.uci.ics.asterix.metadata.declared.ResultSetSinkId;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.AsterixFunctionInfo;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.runtime.formats.FormatUtils;
import edu.uci.ics.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.Counter;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation.BroadcastSide;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

/**
 * Each visit returns a pair of an operator and a variable. The variable
 * corresponds to the new column, if any, added to the tuple flow. E.g., for
 * Unnest, the column is the variable bound to the elements in the list, for
 * Subplan it is null. The first argument of a visit method is the expression
 * which is translated. The second argument of a visit method is the tuple
 * source for the current subtree.
 */

public class AqlExpressionToPlanTranslator extends AbstractAqlTranslator implements
        IAqlExpressionVisitor<Pair<ILogicalOperator, LogicalVariable>, Mutable<ILogicalOperator>> {

    private final AqlMetadataProvider metadataProvider;
    private final TranslationContext context;
    private final String outputDatasetName;
    private final ICompiledDmlStatement stmt;
    private static AtomicLong outputFileID = new AtomicLong(0);
    private static final String OUTPUT_FILE_PREFIX = "OUTPUT_";
    private static LogicalVariable METADATA_DUMMY_VAR = new LogicalVariable(-1);

    public AqlExpressionToPlanTranslator(AqlMetadataProvider metadataProvider, int currentVarCounter,
            String outputDatasetName, ICompiledDmlStatement stmt) {
        this.context = new TranslationContext(new Counter(currentVarCounter));
        this.outputDatasetName = outputDatasetName;
        this.stmt = stmt;
        this.metadataProvider = metadataProvider;
    }

    public int getVarCounter() {
        return context.getVarCounter();
    }

    public ILogicalPlan translate(Query expr) throws AlgebricksException, AsterixException {
        IDataFormat format = FormatUtils.getDefaultFormat();
        format.registerRuntimeFunctions();

        Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this, new MutableObject<ILogicalOperator>(
                new EmptyTupleSourceOperator()));

        ArrayList<Mutable<ILogicalOperator>> globalPlanRoots = new ArrayList<Mutable<ILogicalOperator>>();
        ILogicalOperator topOp = p.first;
        ProjectOperator project = (ProjectOperator) topOp;
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
        } else {
            /** add the collection-to-sequence right before the final project, because dataset only accept non-collection records */
            LogicalVariable seqVar = context.newVar();
            @SuppressWarnings("unchecked")
            /** This assign adds a marker function collection-to-sequence: if the input is a singleton collection, unnest it; otherwise do nothing. */
            AssignOperator assignCollectionToSequence = new AssignOperator(seqVar,
                    new MutableObject<ILogicalExpression>(
                            new ScalarFunctionCallExpression(AsterixBuiltinFunctions
                                    .getAsterixFunctionInfo(AsterixBuiltinFunctions.COLLECTION_TO_SEQUENCE),
                                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(resVar)))));
            assignCollectionToSequence.getInputs().add(
                    new MutableObject<ILogicalOperator>(project.getInputs().get(0).getValue()));
            project.getInputs().get(0).setValue(assignCollectionToSequence);
            project.getVariables().set(0, seqVar);
            resVar = seqVar;

            AqlDataSource targetDatasource = validateDatasetInfo(metadataProvider, stmt.getDataverseName(),
                    stmt.getDatasetName());
            ArrayList<LogicalVariable> vars = new ArrayList<LogicalVariable>();
            ArrayList<Mutable<ILogicalExpression>> exprs = new ArrayList<Mutable<ILogicalExpression>>();
            List<Mutable<ILogicalExpression>> varRefsForLoading = new ArrayList<Mutable<ILogicalExpression>>();
            List<String> partitionKeys = DatasetUtils.getPartitioningKeys(targetDatasource.getDataset());
            for (String keyFieldName : partitionKeys) {
                IFunctionInfo finfoAccess = AsterixBuiltinFunctions
                        .getAsterixFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME);
                @SuppressWarnings("unchecked")
                ScalarFunctionCallExpression f = new ScalarFunctionCallExpression(finfoAccess,
                        new MutableObject<ILogicalExpression>(new VariableReferenceExpression(METADATA_DUMMY_VAR)),
                        new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                                new AString(keyFieldName)))));
                f.substituteVar(METADATA_DUMMY_VAR, resVar);
                exprs.add(new MutableObject<ILogicalExpression>(f));
                LogicalVariable v = context.newVar();
                vars.add(v);
                varRefsForLoading.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v)));
            }
            AssignOperator assign = new AssignOperator(vars, exprs);
            assign.getInputs().add(new MutableObject<ILogicalOperator>(project));
            Mutable<ILogicalExpression> varRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                    resVar));
            ILogicalOperator leafOperator = null;

            switch (stmt.getKind()) {
                case INSERT: {
                    ILogicalOperator insertOp = new InsertDeleteOperator(targetDatasource, varRef, varRefsForLoading,
                            InsertDeleteOperator.Kind.INSERT);
                    insertOp.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                    leafOperator = new SinkOperator();
                    leafOperator.getInputs().add(new MutableObject<ILogicalOperator>(insertOp));
                    break;
                }
                case DELETE: {
                    ILogicalOperator deleteOp = new InsertDeleteOperator(targetDatasource, varRef, varRefsForLoading,
                            InsertDeleteOperator.Kind.DELETE);
                    deleteOp.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                    leafOperator = new SinkOperator();
                    leafOperator.getInputs().add(new MutableObject<ILogicalOperator>(deleteOp));
                    break;
                }
                case BEGIN_FEED: {
                    ILogicalOperator insertOp = new InsertDeleteOperator(targetDatasource, varRef, varRefsForLoading,
                            InsertDeleteOperator.Kind.INSERT);
                    insertOp.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                    leafOperator = new SinkOperator();
                    leafOperator.getInputs().add(new MutableObject<ILogicalOperator>(insertOp));
                    break;
                }
            }
            topOp = leafOperator;
        }
        globalPlanRoots.add(new MutableObject<ILogicalOperator>(topOp));
        ILogicalPlan plan = new ALogicalPlanImpl(globalPlanRoots);
        return plan;
    }

    private AqlDataSource validateDatasetInfo(AqlMetadataProvider metadataProvider, String dataverseName,
            String datasetName) throws AlgebricksException {
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Cannot find dataset " + datasetName + " in dataverse " + dataverseName);
        }

        AqlSourceId sourceId = new AqlSourceId(dataverseName, datasetName);
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = metadataProvider.findType(dataverseName, itemTypeName);
        AqlDataSource dataSource = new AqlDataSource(sourceId, dataset, itemType);
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("Cannot write output to an external dataset.");
        }
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
    public Pair<ILogicalOperator, LogicalVariable> visitForClause(ForClause fc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        LogicalVariable v = context.newVar(fc.getVarExpr());
        Expression inExpr = fc.getInExpr();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(inExpr, tupSource);
        ILogicalOperator returnedOp;

        if (fc.getPosVarExpr() == null) {
            returnedOp = new UnnestOperator(v, new MutableObject<ILogicalExpression>(makeUnnestExpression(eo.first)));
        } else {
            LogicalVariable pVar = context.newVar(fc.getPosVarExpr());
            returnedOp = new UnnestOperator(v, new MutableObject<ILogicalExpression>(makeUnnestExpression(eo.first)),
                    pVar, BuiltinType.AINT32);
        }
        returnedOp.getInputs().add(eo.second);

        return new Pair<ILogicalOperator, LogicalVariable>(returnedOp, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitLetClause(LetClause lc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        LogicalVariable v;
        ILogicalOperator returnedOp;

        switch (lc.getBindingExpr().getKind()) {
            case VARIABLE_EXPRESSION: {
                v = context.newVar(lc.getVarExpr());
                LogicalVariable prev = context.getVar(((VariableExpr) lc.getBindingExpr()).getVar().getId());
                returnedOp = new AssignOperator(v, new MutableObject<ILogicalExpression>(
                        new VariableReferenceExpression(prev)));
                returnedOp.getInputs().add(tupSource);
                break;
            }
            default: {
                v = context.newVar(lc.getVarExpr());
                Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(lc.getBindingExpr(),
                        tupSource);
                returnedOp = new AssignOperator(v, new MutableObject<ILogicalExpression>(eo.first));
                returnedOp.getInputs().add(eo.second);
                break;
            }
        }
        return new Pair<ILogicalOperator, LogicalVariable>(returnedOp, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitFlworExpression(FLWOGRExpression flwor,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        Mutable<ILogicalOperator> flworPlan = tupSource;
        boolean isTop = context.isTopFlwor();
        if (isTop) {
            context.setTopFlwor(false);
        }
        for (Clause c : flwor.getClauseList()) {
            Pair<ILogicalOperator, LogicalVariable> pC = c.accept(this, flworPlan);
            flworPlan = new MutableObject<ILogicalOperator>(pC.first);
        }

        Expression r = flwor.getReturnExpr();
        boolean noFlworClause = flwor.noForClause();

        if (r.getKind() == Kind.VARIABLE_EXPRESSION) {
            VariableExpr v = (VariableExpr) r;
            LogicalVariable var = context.getVar(v.getVar().getId());

            return produceFlwrResult(noFlworClause, isTop, flworPlan, var);

        } else {
            Mutable<ILogicalOperator> baseOp = new MutableObject<ILogicalOperator>(flworPlan.getValue());
            Pair<ILogicalOperator, LogicalVariable> rRes = r.accept(this, baseOp);
            ILogicalOperator rOp = rRes.first;
            ILogicalOperator resOp;
            if (expressionNeedsNoNesting(r)) {
                baseOp.setValue(flworPlan.getValue());
                resOp = rOp;
            } else {
                SubplanOperator s = new SubplanOperator(rOp);
                s.getInputs().add(flworPlan);
                resOp = s;
                baseOp.setValue(new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(s)));
            }
            Mutable<ILogicalOperator> resOpRef = new MutableObject<ILogicalOperator>(resOp);
            return produceFlwrResult(noFlworClause, isTop, resOpRef, rRes.second);
        }
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitFieldAccessor(FieldAccessor fa,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(fa.getExpr(), tupSource);
        LogicalVariable v = context.newVar();
        AbstractFunctionCallExpression fldAccess = new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME));
        fldAccess.getArguments().add(new MutableObject<ILogicalExpression>(p.first));
        ILogicalExpression faExpr = new ConstantExpression(new AsterixConstantValue(new AString(fa.getIdent()
                .getValue())));
        fldAccess.getArguments().add(new MutableObject<ILogicalExpression>(faExpr));
        AssignOperator a = new AssignOperator(v, new MutableObject<ILogicalExpression>(fldAccess));
        a.getInputs().add(p.second);
        return new Pair<ILogicalOperator, LogicalVariable>(a, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitIndexAccessor(IndexAccessor ia,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(ia.getExpr(), tupSource);
        LogicalVariable v = context.newVar();
        AbstractFunctionCallExpression f;
        int i = ia.getIndex();
        if (i == IndexAccessor.ANY) {
            f = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.ANY_COLLECTION_MEMBER));
            f.getArguments().add(new MutableObject<ILogicalExpression>(p.first));
        } else {
            f = new ScalarFunctionCallExpression(FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.GET_ITEM));
            f.getArguments().add(new MutableObject<ILogicalExpression>(p.first));
            f.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(
                            new AsterixConstantValue(new AInt32(i)))));
        }
        AssignOperator a = new AssignOperator(v, new MutableObject<ILogicalExpression>(f));
        a.getInputs().add(p.second);
        return new Pair<ILogicalOperator, LogicalVariable>(a, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitCallExpr(CallExpr fcall, Mutable<ILogicalOperator> tupSource)
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
                    args.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                            ConstantHelper.objectFromLiteral(val.getValue())))));
                    break;
                }
                default: {
                    Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(expr, topOp);
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
        if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)) {
            IFunctionInfo finfo = new AsterixFunctionInfo(signature);
            return new ScalarFunctionCallExpression(finfo, args);
        } else {
            throw new MetadataException(" User defined functions written in " + function.getLanguage()
                    + " are not supported");
        }
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
            UnnestingFunctionCallExpression ufce = new UnnestingFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(fi), args);
            ufce.setReturnsUniqueValues(AsterixBuiltinFunctions.returnsUniqueValues(fi));
            f = ufce;
        } else {
            f = new ScalarFunctionCallExpression(FunctionUtils.getFunctionInfo(fi), args);
        }
        return f;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitFunctionDecl(FunctionDecl fd,
            Mutable<ILogicalOperator> tupSource) {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitGroupbyClause(GroupbyClause gc,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        GroupByOperator gOp = new GroupByOperator();
        Mutable<ILogicalOperator> topOp = tupSource;
        for (GbyVariableExpressionPair ve : gc.getGbyPairList()) {
            LogicalVariable v;
            VariableExpr vexpr = ve.getVar();
            if (vexpr != null) {
                v = context.newVar(vexpr);
            } else {
                v = context.newVar();
            }
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(ve.getExpr(), topOp);
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
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(ve.getExpr(), topOp);
            gOp.addDecorExpression(v, eo.first);
            topOp = eo.second;
        }
        gOp.getInputs().add(topOp);

        for (VariableExpr var : gc.getWithVarList()) {
            LogicalVariable aggVar = context.newVar();
            LogicalVariable oldVar = context.getVar(var);
            List<Mutable<ILogicalExpression>> flArgs = new ArrayList<Mutable<ILogicalExpression>>(1);
            flArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(oldVar)));
            AggregateFunctionCallExpression fListify = AsterixBuiltinFunctions.makeAggregateFunctionExpression(
                    AsterixBuiltinFunctions.LISTIFY, flArgs);
            AggregateOperator agg = new AggregateOperator(mkSingletonArrayList(aggVar),
                    (List) mkSingletonArrayList(new MutableObject<ILogicalExpression>(fListify)));

            agg.getInputs().add(
                    new MutableObject<ILogicalOperator>(new NestedTupleSourceOperator(
                            new MutableObject<ILogicalOperator>(gOp))));
            ILogicalPlan plan = new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(agg));
            gOp.getNestedPlans().add(plan);
            // Hide the variable that was part of the "with", replacing it with
            // the one bound by the aggregation op.
            context.setVar(var, aggVar);
        }

        gOp.getAnnotations().put(OperatorAnnotations.USE_HASH_GROUP_BY, gc.hasHashGroupByHint());
        return new Pair<ILogicalOperator, LogicalVariable>(gOp, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitIfExpr(IfExpr ifexpr, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        // In the most general case, IfThenElse is translated in the following
        // way.
        //
        // We assign the result of the condition to one variable varCond.
        // We create one subplan which contains the plan for the "then" branch,
        // on top of which there is a selection whose condition is varCond.
        // Similarly, we create one subplan for the "else" branch, in which the
        // selection is not(varCond).
        // Finally, we concatenate the results. (??)

        Pair<ILogicalOperator, LogicalVariable> pCond = ifexpr.getCondExpr().accept(this, tupSource);
        ILogicalOperator opCond = pCond.first;
        LogicalVariable varCond = pCond.second;

        SubplanOperator sp = new SubplanOperator();
        Mutable<ILogicalOperator> nestedSource = new MutableObject<ILogicalOperator>(new NestedTupleSourceOperator(
                new MutableObject<ILogicalOperator>(sp)));

        Pair<ILogicalOperator, LogicalVariable> pThen = ifexpr.getThenExpr().accept(this, nestedSource);
        SelectOperator sel1 = new SelectOperator(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                varCond)));
        sel1.getInputs().add(new MutableObject<ILogicalOperator>(pThen.first));

        Pair<ILogicalOperator, LogicalVariable> pElse = ifexpr.getElseExpr().accept(this, nestedSource);
        AbstractFunctionCallExpression notVarCond = new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AlgebricksBuiltinFunctions.NOT), new MutableObject<ILogicalExpression>(
                        new VariableReferenceExpression(varCond)));
        SelectOperator sel2 = new SelectOperator(new MutableObject<ILogicalExpression>(notVarCond));
        sel2.getInputs().add(new MutableObject<ILogicalOperator>(pElse.first));

        ILogicalPlan p1 = new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(sel1));
        sp.getNestedPlans().add(p1);
        ILogicalPlan p2 = new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(sel2));
        sp.getNestedPlans().add(p2);

        Mutable<ILogicalOperator> opCondRef = new MutableObject<ILogicalOperator>(opCond);
        sp.getInputs().add(opCondRef);

        LogicalVariable resV = context.newVar();
        AbstractFunctionCallExpression concatNonNull = new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CONCAT_NON_NULL),
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(pThen.second)),
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(pElse.second)));
        AssignOperator a = new AssignOperator(resV, new MutableObject<ILogicalExpression>(concatNonNull));
        a.getInputs().add(new MutableObject<ILogicalOperator>(sp));

        return new Pair<ILogicalOperator, LogicalVariable>(a, resV);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitLiteralExpr(LiteralExpr l, Mutable<ILogicalOperator> tupSource) {
        LogicalVariable var = context.newVar();
        AssignOperator a = new AssignOperator(var, new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(ConstantHelper.objectFromLiteral(l.getValue())))));
        if (tupSource != null) {
            a.getInputs().add(tupSource);
        }
        return new Pair<ILogicalOperator, LogicalVariable>(a, var);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitOperatorExpr(OperatorExpr op,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        ArrayList<OperatorType> ops = op.getOpList();
        int nOps = ops.size();

        if (nOps > 0 && (ops.get(0) == OperatorType.AND || ops.get(0) == OperatorType.OR)) {
            return visitAndOrOperator(op, tupSource);
        }

        ArrayList<Expression> exprs = op.getExprList();

        Mutable<ILogicalOperator> topOp = tupSource;

        ILogicalExpression currExpr = null;
        for (int i = 0; i <= nOps; i++) {

            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(exprs.get(i), topOp);
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
                        ((AbstractFunctionCallExpression) currExpr).getArguments().add(
                                new MutableObject<ILogicalExpression>(e));
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
                        ((AbstractFunctionCallExpression) currExpr).getArguments().add(
                                new MutableObject<ILogicalExpression>(e));
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
                    ((AbstractFunctionCallExpression) currExpr).getAnnotations().put(
                            BroadcastExpressionAnnotation.BROADCAST_ANNOTATION_KEY, bcast);
                }
            }
        }

        // Add hints as annotations.
        if (op.hasHints() && currExpr instanceof AbstractFunctionCallExpression) {
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
    public Pair<ILogicalOperator, LogicalVariable> visitOrderbyClause(OrderbyClause oc,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {

        OrderOperator ord = new OrderOperator();
        Iterator<OrderModifier> modifIter = oc.getModifierList().iterator();
        Mutable<ILogicalOperator> topOp = tupSource;
        for (Expression e : oc.getOrderbyList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(e, topOp);
            OrderModifier m = modifIter.next();
            OrderOperator.IOrder comp = (m == OrderModifier.ASC) ? OrderOperator.ASC_ORDER : OrderOperator.DESC_ORDER;
            ord.getOrderExpressions()
                    .add(new Pair<IOrder, Mutable<ILogicalExpression>>(comp, new MutableObject<ILogicalExpression>(
                            p.first)));
            topOp = p.second;
        }
        ord.getInputs().add(topOp);
        if (oc.getNumTuples() > 0) {
            ord.getAnnotations().put(OperatorAnnotations.CARDINALITY, oc.getNumTuples());
        }
        if (oc.getNumFrames() > 0) {
            ord.getAnnotations().put(OperatorAnnotations.MAX_NUMBER_FRAMES, oc.getNumFrames());
        }
        return new Pair<ILogicalOperator, LogicalVariable>(ord, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitQuantifiedExpression(QuantifiedExpression qe,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        Mutable<ILogicalOperator> topOp = tupSource;

        ILogicalOperator firstOp = null;
        Mutable<ILogicalOperator> lastOp = null;

        for (QuantifiedPair qt : qe.getQuantifiedList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo1 = aqlExprToAlgExpression(qt.getExpr(), topOp);
            topOp = eo1.second;
            LogicalVariable uVar = context.newVar(qt.getVarExpr());
            ILogicalOperator u = new UnnestOperator(uVar, new MutableObject<ILogicalExpression>(
                    makeUnnestExpression(eo1.first)));

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

        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo2 = aqlExprToAlgExpression(qe.getSatisfiesExpr(), topOp);

        AggregateFunctionCallExpression fAgg;
        SelectOperator s;
        if (qe.getQuantifier() == Quantifier.SOME) {
            s = new SelectOperator(new MutableObject<ILogicalExpression>(eo2.first));
            s.getInputs().add(eo2.second);
            fAgg = AsterixBuiltinFunctions.makeAggregateFunctionExpression(AsterixBuiltinFunctions.NON_EMPTY_STREAM,
                    new ArrayList<Mutable<ILogicalExpression>>());
        } else { // EVERY
            List<Mutable<ILogicalExpression>> satExprList = new ArrayList<Mutable<ILogicalExpression>>(1);
            satExprList.add(new MutableObject<ILogicalExpression>(eo2.first));
            s = new SelectOperator(new MutableObject<ILogicalExpression>(new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AlgebricksBuiltinFunctions.NOT), satExprList)));
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
    public Pair<ILogicalOperator, LogicalVariable> visitQuery(Query q, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        return q.getBody().accept(this, tupSource);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitRecordConstructor(RecordConstructor rc,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        AbstractFunctionCallExpression f = new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR));
        LogicalVariable v1 = context.newVar();
        AssignOperator a = new AssignOperator(v1, new MutableObject<ILogicalExpression>(f));
        Mutable<ILogicalOperator> topOp = tupSource;
        for (FieldBinding fb : rc.getFbList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo1 = aqlExprToAlgExpression(fb.getLeftExpr(), topOp);
            f.getArguments().add(new MutableObject<ILogicalExpression>(eo1.first));
            topOp = eo1.second;
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo2 = aqlExprToAlgExpression(fb.getRightExpr(), topOp);
            f.getArguments().add(new MutableObject<ILogicalExpression>(eo2.first));
            topOp = eo2.second;
        }
        a.getInputs().add(topOp);
        return new Pair<ILogicalOperator, LogicalVariable>(a, v1);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitListConstructor(ListConstructor lc,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        FunctionIdentifier fid = (lc.getType() == Type.ORDERED_LIST_CONSTRUCTOR) ? AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR
                : AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR;
        AbstractFunctionCallExpression f = new ScalarFunctionCallExpression(FunctionUtils.getFunctionInfo(fid));
        LogicalVariable v1 = context.newVar();
        AssignOperator a = new AssignOperator(v1, new MutableObject<ILogicalExpression>(f));
        Mutable<ILogicalOperator> topOp = tupSource;
        for (Expression expr : lc.getExprList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(expr, topOp);
            f.getArguments().add(new MutableObject<ILogicalExpression>(eo.first));
            topOp = eo.second;
        }
        a.getInputs().add(topOp);
        return new Pair<ILogicalOperator, LogicalVariable>(a, v1);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitUnaryExpr(UnaryExpr u, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Expression expr = u.getExpr();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(expr, tupSource);
        LogicalVariable v1 = context.newVar();
        AssignOperator a;
        if (u.getSign() == Sign.POSITIVE) {
            a = new AssignOperator(v1, new MutableObject<ILogicalExpression>(eo.first));
        } else {
            AbstractFunctionCallExpression m = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.NUMERIC_UNARY_MINUS));
            m.getArguments().add(new MutableObject<ILogicalExpression>(eo.first));
            a = new AssignOperator(v1, new MutableObject<ILogicalExpression>(m));
        }
        a.getInputs().add(eo.second);
        return new Pair<ILogicalOperator, LogicalVariable>(a, v1);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitVariableExpr(VariableExpr v, Mutable<ILogicalOperator> tupSource) {
        // Should we ever get to this method?
        LogicalVariable var = context.newVar();
        LogicalVariable oldV = context.getVar(v.getVar().getId());
        AssignOperator a = new AssignOperator(var, new MutableObject<ILogicalExpression>(
                new VariableReferenceExpression(oldV)));
        a.getInputs().add(tupSource);
        return new Pair<ILogicalOperator, LogicalVariable>(a, var);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitWhereClause(WhereClause w, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(w.getWhereExpr(), tupSource);
        SelectOperator s = new SelectOperator(new MutableObject<ILogicalExpression>(p.first));
        s.getInputs().add(p.second);

        return new Pair<ILogicalOperator, LogicalVariable>(s, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitLimitClause(LimitClause lc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p1 = aqlExprToAlgExpression(lc.getLimitExpr(), tupSource);
        LimitOperator opLim;
        Expression offset = lc.getOffset();
        if (offset != null) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p2 = aqlExprToAlgExpression(offset, p1.second);
            opLim = new LimitOperator(p1.first, p2.first);
            opLim.getInputs().add(p2.second);
        } else {
            opLim = new LimitOperator(p1.first);
            opLim.getInputs().add(p1.second);
        }
        return new Pair<ILogicalOperator, LogicalVariable>(opLim, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitDistinctClause(DistinctClause dc,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        List<Mutable<ILogicalExpression>> exprList = new ArrayList<Mutable<ILogicalExpression>>();
        Mutable<ILogicalOperator> input = null;
        for (Expression expr : dc.getDistinctByExpr()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(expr, tupSource);
            exprList.add(new MutableObject<ILogicalExpression>(p.first));
            input = p.second;
        }
        DistinctOperator opDistinct = new DistinctOperator(exprList);
        opDistinct.getInputs().add(input);
        return new Pair<ILogicalOperator, LogicalVariable>(opDistinct, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitUnionExpr(UnionExpr unionExpr,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        Mutable<ILogicalOperator> ts = tupSource;
        ILogicalOperator lastOp = null;
        LogicalVariable lastVar = null;
        boolean first = true;
        for (Expression e : unionExpr.getExprs()) {
            if (first) {
                first = false;
            } else {
                ts = new MutableObject<ILogicalOperator>(new EmptyTupleSourceOperator());
            }
            Pair<ILogicalOperator, LogicalVariable> p1 = e.accept(this, ts);
            if (lastOp == null) {
                lastOp = p1.first;
                lastVar = p1.second;
            } else {
                LogicalVariable unnestVar1 = context.newVar();
                UnnestOperator unnest1 = new UnnestOperator(unnestVar1, new MutableObject<ILogicalExpression>(
                        makeUnnestExpression(new VariableReferenceExpression(lastVar))));
                unnest1.getInputs().add(new MutableObject<ILogicalOperator>(lastOp));
                LogicalVariable unnestVar2 = context.newVar();
                UnnestOperator unnest2 = new UnnestOperator(unnestVar2, new MutableObject<ILogicalExpression>(
                        makeUnnestExpression(new VariableReferenceExpression(p1.second))));
                unnest2.getInputs().add(new MutableObject<ILogicalOperator>(p1.first));
                List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap = new ArrayList<Triple<LogicalVariable, LogicalVariable, LogicalVariable>>(
                        1);
                LogicalVariable resultVar = context.newVar();
                Triple<LogicalVariable, LogicalVariable, LogicalVariable> triple = new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(
                        unnestVar1, unnestVar2, resultVar);
                varMap.add(triple);
                UnionAllOperator unionOp = new UnionAllOperator(varMap);
                unionOp.getInputs().add(new MutableObject<ILogicalOperator>(unnest1));
                unionOp.getInputs().add(new MutableObject<ILogicalOperator>(unnest2));
                lastVar = resultVar;
                lastOp = unionOp;
            }
        }
        LogicalVariable aggVar = context.newVar();
        ArrayList<LogicalVariable> aggregVars = new ArrayList<LogicalVariable>(1);
        aggregVars.add(aggVar);
        List<Mutable<ILogicalExpression>> afcExprs = new ArrayList<Mutable<ILogicalExpression>>(1);
        afcExprs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(lastVar)));
        AggregateFunctionCallExpression afc = AsterixBuiltinFunctions.makeAggregateFunctionExpression(
                AsterixBuiltinFunctions.LISTIFY, afcExprs);
        ArrayList<Mutable<ILogicalExpression>> aggregExprs = new ArrayList<Mutable<ILogicalExpression>>(1);
        aggregExprs.add(new MutableObject<ILogicalExpression>(afc));
        AggregateOperator agg = new AggregateOperator(aggregVars, aggregExprs);
        agg.getInputs().add(new MutableObject<ILogicalOperator>(lastOp));
        return new Pair<ILogicalOperator, LogicalVariable>(agg, aggVar);
    }

    private AbstractFunctionCallExpression createComparisonExpression(OperatorType t) {
        FunctionIdentifier fi = operatorTypeToFunctionIdentifier(t);
        IFunctionInfo finfo = FunctionUtils.getFunctionInfo(fi);
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

    private AbstractFunctionCallExpression createFunctionCallExpressionForBuiltinOperator(OperatorType t)
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
        return new ScalarFunctionCallExpression(FunctionUtils.getFunctionInfo(fid));
    }

    private static boolean hasOnlyChild(ILogicalOperator parent, Mutable<ILogicalOperator> childCandidate) {
        List<Mutable<ILogicalOperator>> inp = parent.getInputs();
        if (inp == null || inp.size() != 1) {
            return false;
        }
        return inp.get(0) == childCandidate;
    }

    private Pair<ILogicalExpression, Mutable<ILogicalOperator>> aqlExprToAlgExpression(Expression expr,
            Mutable<ILogicalOperator> topOp) throws AsterixException {
        switch (expr.getKind()) {
            case VARIABLE_EXPRESSION: {
                VariableReferenceExpression ve = new VariableReferenceExpression(context.getVar(((VariableExpr) expr)
                        .getVar().getId()));
                return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(ve, topOp);
            }
            case LITERAL_EXPRESSION: {
                LiteralExpr val = (LiteralExpr) expr;
                return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(new ConstantExpression(
                        new AsterixConstantValue(ConstantHelper.objectFromLiteral(val.getValue()))), topOp);
            }
            default: {
                // Mutable<ILogicalOperator> src = new
                // Mutable<ILogicalOperator>();
                // Mutable<ILogicalOperator> src = topOp;
                if (expressionNeedsNoNesting(expr)) {
                    Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this, topOp);
                    ILogicalExpression exp = ((AssignOperator) p.first).getExpressions().get(0).getValue();
                    return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(exp, p.first.getInputs().get(0));
                } else {
                    Mutable<ILogicalOperator> src = new MutableObject<ILogicalOperator>();

                    Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this, src);

                    if (((AbstractLogicalOperator) p.first).getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                        src.setValue(topOp.getValue());
                        Mutable<ILogicalOperator> top2 = new MutableObject<ILogicalOperator>(p.first);
                        return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(new VariableReferenceExpression(
                                p.second), top2);
                    } else {
                        SubplanOperator s = new SubplanOperator();
                        s.getInputs().add(topOp);
                        src.setValue(new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(s)));
                        Mutable<ILogicalOperator> planRoot = new MutableObject<ILogicalOperator>(p.first);
                        s.setRootOp(planRoot);
                        return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(new VariableReferenceExpression(
                                p.second), new MutableObject<ILogicalOperator>(s));
                    }
                }
            }
        }

    }

    private Pair<ILogicalOperator, LogicalVariable> produceFlwrResult(boolean noForClause, boolean isTop,
            Mutable<ILogicalOperator> resOpRef, LogicalVariable resVar) {
        if (isTop) {
            ProjectOperator pr = new ProjectOperator(resVar);
            pr.getInputs().add(resOpRef);
            return new Pair<ILogicalOperator, LogicalVariable>(pr, resVar);

        } else if (noForClause) {
            return new Pair<ILogicalOperator, LogicalVariable>(resOpRef.getValue(), resVar);
        } else {
            return aggListify(resVar, resOpRef, false);
        }
    }

    private Pair<ILogicalOperator, LogicalVariable> aggListify(LogicalVariable var, Mutable<ILogicalOperator> opRef,
            boolean bProject) {
        AggregateFunctionCallExpression funAgg = AsterixBuiltinFunctions.makeAggregateFunctionExpression(
                AsterixBuiltinFunctions.LISTIFY, new ArrayList<Mutable<ILogicalExpression>>());
        funAgg.getArguments().add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var)));
        LogicalVariable varListified = context.newVar();
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

    private Pair<ILogicalOperator, LogicalVariable> visitAndOrOperator(OperatorExpr op,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        ArrayList<OperatorType> ops = op.getOpList();
        int nOps = ops.size();

        ArrayList<Expression> exprs = op.getExprList();

        Mutable<ILogicalOperator> topOp = tupSource;

        OperatorType opLogical = ops.get(0);
        AbstractFunctionCallExpression f = createFunctionCallExpressionForBuiltinOperator(opLogical);

        for (int i = 0; i <= nOps; i++) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(exprs.get(i), topOp);
            topOp = p.second;
            // now look at the operator
            if (i < nOps) {
                if (ops.get(i) != opLogical) {
                    throw new TranslationException("Unexpected operator " + ops.get(i)
                            + " in an OperatorExpr starting with " + opLogical);
                }
            }
            f.getArguments().add(new MutableObject<ILogicalExpression>(p.first));
        }

        LogicalVariable assignedVar = context.newVar();
        AssignOperator a = new AssignOperator(assignedVar, new MutableObject<ILogicalExpression>(f));
        a.getInputs().add(topOp);

        return new Pair<ILogicalOperator, LogicalVariable>(a, assignedVar);

    }

    private static boolean expressionNeedsNoNesting(Expression expr) {
        Kind k = expr.getKind();
        return k == Kind.LITERAL_EXPRESSION || k == Kind.LIST_CONSTRUCTOR_EXPRESSION
                || k == Kind.RECORD_CONSTRUCTOR_EXPRESSION || k == Kind.VARIABLE_EXPRESSION
                || k == Kind.CALL_EXPRESSION || k == Kind.OP_EXPRESSION || k == Kind.FIELD_ACCESSOR_EXPRESSION
                || k == Kind.INDEX_ACCESSOR_EXPRESSION || k == Kind.UNARY_EXPRESSION;
    }

    private <T> ArrayList<T> mkSingletonArrayList(T item) {
        ArrayList<T> array = new ArrayList<T>(1);
        array.add(item);
        return array;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitTypeDecl(TypeDecl td, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitRecordTypeDefiniton(RecordTypeDefinition tre,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitTypeReferenceExpression(TypeReferenceExpression tre,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitNodegroupDecl(NodegroupDecl ngd, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitLoadFromFileStatement(LoadFromFileStatement stmtLoad,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitDropStatement(DropStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitControlFeedStatement(ControlFeedStatement del,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitCreateIndexStatement(CreateIndexStatement cis,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitOrderedListTypeDefiniton(OrderedListTypeDefinition olte,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitUnorderedListTypeDefiniton(UnorderedListTypeDefinition ulte,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    private ILogicalExpression makeUnnestExpression(ILogicalExpression expr) {
        switch (expr.getExpressionTag()) {
            case VARIABLE: {
                return new UnnestingFunctionCallExpression(
                        FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION),
                        new MutableObject<ILogicalExpression>(expr));
            }
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                if (fce.getKind() == FunctionKind.UNNEST) {
                    return expr;
                } else {
                    return new UnnestingFunctionCallExpression(
                            FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION),
                            new MutableObject<ILogicalExpression>(expr));
                }
            }
            default: {
                return expr;
            }
        }
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitInsertStatement(InsertStatement insert,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitDeleteStatement(DeleteStatement del,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitUpdateStatement(UpdateStatement update,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitUpdateClause(UpdateClause del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitDataverseDecl(DataverseDecl dv, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitDatasetDecl(DatasetDecl dd, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitSetStatement(SetStatement ss, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitWriteStatement(WriteStatement ws, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitCreateDataverseStatement(CreateDataverseStatement del,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitIndexDropStatement(IndexDropStatement del,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitNodeGroupDropStatement(NodeGroupDropStatement del,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitDataverseDropStatement(DataverseDropStatement del,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitTypeDropStatement(TypeDropStatement del,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CreateFunctionStatement cfs, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitFunctionDropStatement(FunctionDropStatement del,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitBeginFeedStatement(BeginFeedStatement bf,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitCompactStatement(CompactStatement del,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }
}
