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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.clause.JoinClause;
import org.apache.asterix.lang.aql.clause.MetaVariableClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.MetaVariableExpr;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.aql.visitor.base.IAQLPlusVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.OrderbyClause.OrderModifier;
import org.apache.asterix.lang.common.clause.UpdateClause;
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
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.QuantifiedExpression.Quantifier;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.UnaryExpr.Sign;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.CompactStatement;
import org.apache.asterix.lang.common.statement.ConnectFeedStatement;
import org.apache.asterix.lang.common.statement.CreateDataverseStatement;
import org.apache.asterix.lang.common.statement.CreateFeedPolicyStatement;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.lang.common.statement.CreatePrimaryFeedStatement;
import org.apache.asterix.lang.common.statement.CreateSecondaryFeedStatement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.DisconnectFeedStatement;
import org.apache.asterix.lang.common.statement.DropStatement;
import org.apache.asterix.lang.common.statement.FeedDropStatement;
import org.apache.asterix.lang.common.statement.FeedPolicyDropStatement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.LoadStatement;
import org.apache.asterix.lang.common.statement.NodeGroupDropStatement;
import org.apache.asterix.lang.common.statement.NodegroupDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.common.statement.TypeDecl;
import org.apache.asterix.lang.common.statement.TypeDropStatement;
import org.apache.asterix.lang.common.statement.UpdateStatement;
import org.apache.asterix.lang.common.statement.WriteStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.FileSplitDataSink;
import org.apache.asterix.metadata.declared.FileSplitSinkId;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.AsterixFunctionInfo;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.asterix.translator.util.FunctionCollection;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation.BroadcastSide;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

/**
 * Each visit returns a pair of an operator and a variable. The variable
 * corresponds to the new column, if any, added to the tuple flow. E.g., for
 * Unnest, the column is the variable bound to the elements in the list, for
 * Subplan it is null. The first argument of a visit method is the expression
 * which is translated. The second argument of a visit method is the tuple
 * source for the current subtree.
 */

public class AqlPlusExpressionToPlanTranslator extends AbstractLangTranslator
        implements IAQLPlusVisitor<Pair<ILogicalOperator, LogicalVariable>, Mutable<ILogicalOperator>> {

    private static final Logger LOGGER = Logger.getLogger(AqlPlusExpressionToPlanTranslator.class.getName());

    private class MetaScopeLogicalVariable {
        private HashMap<Identifier, LogicalVariable> map = new HashMap<Identifier, LogicalVariable>();

        public VariableReferenceExpression getVariableReferenceExpression(Identifier id) throws AsterixException {
            LogicalVariable var = map.get(id);
            LOGGER.fine("get:" + id + ":" + var);
            if (var == null) {
                throw new AsterixException("Identifier " + id + " not found in AQL+ meta-scope.");
            }
            return new VariableReferenceExpression(var);
        }

        public void put(Identifier id, LogicalVariable var) {
            LOGGER.fine("put:" + id + ":" + var);
            map.put(id, var);
        }
    }

    private class MetaScopeILogicalOperator {
        private HashMap<Identifier, ILogicalOperator> map = new HashMap<Identifier, ILogicalOperator>();

        public ILogicalOperator get(Identifier id) throws AsterixException {
            ILogicalOperator op = map.get(id);
            if (op == null) {
                throw new AsterixException("Identifier " + id + " not found in AQL+ meta-scope.");
            }
            return op;
        }

        public void put(Identifier id, ILogicalOperator op) {
            LOGGER.fine("put:" + id + ":" + op);
            map.put(id, op);
        }
    }

    private final JobId jobId;
    private TranslationContext context;
    private String outputDatasetName;
    private ICompiledDmlStatement stmt;
    private AqlMetadataProvider metadataProvider;

    private MetaScopeLogicalVariable metaScopeExp = new MetaScopeLogicalVariable();
    private MetaScopeILogicalOperator metaScopeOp = new MetaScopeILogicalOperator();
    private static LogicalVariable METADATA_DUMMY_VAR = new LogicalVariable(-1);

    public AqlPlusExpressionToPlanTranslator(JobId jobId, AqlMetadataProvider metadataProvider,
            Counter currentVarCounter, String outputDatasetName, ICompiledDmlStatement stmt) {
        this.jobId = jobId;
        this.metadataProvider = metadataProvider;
        this.context = new TranslationContext(currentVarCounter);
        this.outputDatasetName = outputDatasetName;
        this.stmt = stmt;
        this.context.setTopFlwor(false);
    }

    public int getVarCounter() {
        return context.getVarCounter();
    }

    public ILogicalPlan translate(Query expr) throws AlgebricksException, AsterixException {
        return translate(expr, null);
    }

    public ILogicalPlan translate(Query expr, AqlMetadataProvider metadata)
            throws AlgebricksException, AsterixException {
        IDataFormat format = metadata.getFormat();
        if (format == null) {
            throw new AlgebricksException("Data format has not been set.");
        }
        format.registerRuntimeFunctions(FunctionCollection.getFunctionDescriptorFactories());
        Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this,
                new MutableObject<ILogicalOperator>(new EmptyTupleSourceOperator()));

        ArrayList<Mutable<ILogicalOperator>> globalPlanRoots = new ArrayList<Mutable<ILogicalOperator>>();

        boolean isTransactionalWrite = false;
        ILogicalOperator topOp = p.first;
        ProjectOperator project = (ProjectOperator) topOp;
        LogicalVariable resVar = project.getVariables().get(0);
        if (outputDatasetName == null) {
            List<Mutable<ILogicalExpression>> writeExprList = new ArrayList<Mutable<ILogicalExpression>>(1);
            writeExprList.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(resVar)));
            FileSplitSinkId fssi = new FileSplitSinkId(metadata.getOutputFile());
            FileSplitDataSink sink = new FileSplitDataSink(fssi, null);
            topOp = new WriteOperator(writeExprList, sink);
            topOp.getInputs().add(new MutableObject<ILogicalOperator>(project));
        } else {
            Dataset dataset = metadata.findDataset(stmt.getDataverseName(), outputDatasetName);
            if (dataset == null) {
                throw new AlgebricksException("Cannot find dataset " + outputDatasetName);
            }
            if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
                throw new AlgebricksException("Cannot write output to an external dataset.");
            }
            ARecordType itemType = (ARecordType) metadata.findType(dataset.getItemTypeDataverseName(),
                    dataset.getItemTypeName());
            List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            ArrayList<LogicalVariable> vars = new ArrayList<LogicalVariable>();
            ArrayList<Mutable<ILogicalExpression>> exprs = new ArrayList<Mutable<ILogicalExpression>>();
            List<Mutable<ILogicalExpression>> varRefsForLoading = new ArrayList<Mutable<ILogicalExpression>>();
            for (List<String> partitioningKey : partitioningKeys) {
                Triple<IScalarEvaluatorFactory, ScalarFunctionCallExpression, IAType> partitioner = format
                        .partitioningEvaluatorFactory(itemType, partitioningKey);
                AbstractFunctionCallExpression f = partitioner.second.cloneExpression();
                f.substituteVar(METADATA_DUMMY_VAR, resVar);
                exprs.add(new MutableObject<ILogicalExpression>(f));
                LogicalVariable v = context.newVar();
                vars.add(v);
                varRefsForLoading.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v)));
            }
            AssignOperator assign = new AssignOperator(vars, exprs);
            assign.getInputs().add(new MutableObject<ILogicalOperator>(project));
        }

        globalPlanRoots.add(new MutableObject<ILogicalOperator>(topOp));
        ILogicalPlan plan = new ALogicalPlanImpl(globalPlanRoots);
        return plan;
    }

    public ILogicalPlan translate(List<Clause> clauses) throws AlgebricksException, AsterixException {

        if (clauses == null) {
            return null;
        }

        Mutable<ILogicalOperator> opRef = new MutableObject<ILogicalOperator>(new EmptyTupleSourceOperator());
        Pair<ILogicalOperator, LogicalVariable> p = null;
        for (Clause c : clauses) {
            p = c.accept(this, opRef);
            opRef = new MutableObject<ILogicalOperator>(p.first);
        }

        ArrayList<Mutable<ILogicalOperator>> globalPlanRoots = new ArrayList<Mutable<ILogicalOperator>>();

        ILogicalOperator topOp = p.first;

        globalPlanRoots.add(new MutableObject<ILogicalOperator>(topOp));
        ILogicalPlan plan = new ALogicalPlanImpl(globalPlanRoots);
        return plan;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(ForClause fc, Mutable<ILogicalOperator> tupSource)
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
                    pVar, BuiltinType.AINT32, new AqlPositionWriter());
        }
        returnedOp.getInputs().add(eo.second);

        return new Pair<ILogicalOperator, LogicalVariable>(returnedOp, v);
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
                Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(lc.getBindingExpr(),
                        tupSource);
                v = context.newVar(lc.getVarExpr());
                returnedOp = new AssignOperator(v, new MutableObject<ILogicalExpression>(eo.first));
                returnedOp.getInputs().add(eo.second);
                break;
            }
        }
        return new Pair<ILogicalOperator, LogicalVariable>(returnedOp, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FLWOGRExpression flwor, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
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
    public Pair<ILogicalOperator, LogicalVariable> visit(FieldAccessor fa, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(fa.getExpr(), tupSource);
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
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(ia.getExpr(), tupSource);
        LogicalVariable v = context.newVar();
        AbstractFunctionCallExpression f;
        if (ia.isAny()) {
            f = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.ANY_COLLECTION_MEMBER));
            f.getArguments().add(new MutableObject<ILogicalExpression>(p.first));
        } else {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> indexPair = aqlExprToAlgExpression(ia.getIndexExpr(),
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

        FunctionIdentifier fi = new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, signature.getName());
        AsterixFunctionInfo afi = AsterixBuiltinFunctions.lookupFunction(fi);
        FunctionIdentifier builtinAquafi = afi == null ? null : afi.getFunctionIdentifier();

        if (builtinAquafi != null) {
            fi = builtinAquafi;
        } else {
            fi = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, signature.getName());
            FunctionIdentifier builtinAsterixFi = AsterixBuiltinFunctions.getBuiltinFunctionIdentifier(fi);
            if (builtinAsterixFi != null) {
                fi = builtinAsterixFi;
            }
        }
        AbstractFunctionCallExpression f;
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
        AssignOperator op = new AssignOperator(v, new MutableObject<ILogicalExpression>(f));
        if (topOp != null) {
            op.getInputs().add(topOp);
        }

        return new Pair<ILogicalOperator, LogicalVariable>(op, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FunctionDecl fd, Mutable<ILogicalOperator> tupSource) {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(GroupbyClause gc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
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
        // Finally, we concatenate the results. (??)

        Pair<ILogicalOperator, LogicalVariable> pCond = ifexpr.getCondExpr().accept(this, tupSource);
        ILogicalOperator opCond = pCond.first;
        LogicalVariable varCond = pCond.second;

        SubplanOperator sp = new SubplanOperator();
        Mutable<ILogicalOperator> nestedSource = new MutableObject<ILogicalOperator>(
                new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(sp)));

        Pair<ILogicalOperator, LogicalVariable> pThen = ifexpr.getThenExpr().accept(this, nestedSource);
        SelectOperator sel1 = new SelectOperator(
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(varCond)), false, null);
        sel1.getInputs().add(new MutableObject<ILogicalOperator>(pThen.first));

        Pair<ILogicalOperator, LogicalVariable> pElse = ifexpr.getElseExpr().accept(this, nestedSource);
        AbstractFunctionCallExpression notVarCond = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.NOT),
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(varCond)));
        SelectOperator sel2 = new SelectOperator(new MutableObject<ILogicalExpression>(notVarCond), false, null);
        sel2.getInputs().add(new MutableObject<ILogicalOperator>(pElse.first));

        ILogicalPlan p1 = new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(sel1));
        sp.getNestedPlans().add(p1);
        ILogicalPlan p2 = new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(sel2));
        sp.getNestedPlans().add(p2);

        Mutable<ILogicalOperator> opCondRef = new MutableObject<ILogicalOperator>(opCond);
        sp.getInputs().add(opCondRef);

        LogicalVariable resV = context.newVar();
        AbstractFunctionCallExpression concatNonNull = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.CONCAT_NON_NULL),
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(pThen.second)),
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(pElse.second)));
        AssignOperator a = new AssignOperator(resV, new MutableObject<ILogicalExpression>(concatNonNull));
        a.getInputs().add(new MutableObject<ILogicalOperator>(sp));

        return new Pair<ILogicalOperator, LogicalVariable>(a, resV);
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
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(e, topOp);
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
        return new Pair<ILogicalOperator, LogicalVariable>(ord, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(QuantifiedExpression qe, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Mutable<ILogicalOperator> topOp = tupSource;

        ILogicalOperator firstOp = null;
        Mutable<ILogicalOperator> lastOp = null;

        for (QuantifiedPair qt : qe.getQuantifiedList()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo1 = aqlExprToAlgExpression(qt.getExpr(), topOp);
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

        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo2 = aqlExprToAlgExpression(qe.getSatisfiesExpr(), topOp);

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
    public Pair<ILogicalOperator, LogicalVariable> visit(ListConstructor lc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        FunctionIdentifier fid = (lc.getType() == Type.ORDERED_LIST_CONSTRUCTOR)
                ? AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR : AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR;
        AbstractFunctionCallExpression f = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(fid));
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
    public Pair<ILogicalOperator, LogicalVariable> visit(UnaryExpr u, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Expression expr = u.getExpr();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(expr, tupSource);
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
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(w.getWhereExpr(), tupSource);
        SelectOperator s = new SelectOperator(new MutableObject<ILogicalExpression>(p.first), false, null);
        s.getInputs().add(p.second);

        return new Pair<ILogicalOperator, LogicalVariable>(s, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(LimitClause lc, Mutable<ILogicalOperator> tupSource)
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
    public Pair<ILogicalOperator, LogicalVariable> visit(DistinctClause dc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
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
    public Pair<ILogicalOperator, LogicalVariable> visit(UnionExpr unionExpr, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
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
        AggregateFunctionCallExpression afc = AsterixBuiltinFunctions
                .makeAggregateFunctionExpression(AsterixBuiltinFunctions.LISTIFY, afcExprs);
        ArrayList<Mutable<ILogicalExpression>> aggregExprs = new ArrayList<Mutable<ILogicalExpression>>(1);
        aggregExprs.add(new MutableObject<ILogicalExpression>(afc));
        AggregateOperator agg = new AggregateOperator(aggregVars, aggregExprs);
        agg.getInputs().add(new MutableObject<ILogicalOperator>(lastOp));
        return new Pair<ILogicalOperator, LogicalVariable>(agg, aggVar);
    }

    private AbstractFunctionCallExpression createComparisonExpression(OperatorType t) {
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
        return new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(fid));
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
                VariableReferenceExpression ve = new VariableReferenceExpression(
                        context.getVar(((VariableExpr) expr).getVar().getId()));
                return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(ve, topOp);
            }
            case METAVARIABLE_EXPRESSION: {
                ILogicalExpression le = metaScopeExp.getVariableReferenceExpression(((VariableExpr) expr).getVar());
                return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(le, topOp);
            }
            case LITERAL_EXPRESSION: {
                LiteralExpr val = (LiteralExpr) expr;
                return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(new ConstantExpression(
                        new AsterixConstantValue(ConstantHelper.objectFromLiteral(val.getValue()))), topOp);
            }
            default: {
                // Mutable<ILogicalExpression> src = new
                // Mutable<ILogicalExpression>();
                // Mutable<ILogicalExpression> src = topOp;
                if (expressionNeedsNoNesting(expr)) {
                    Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this, topOp);
                    ILogicalExpression exp = ((AssignOperator) p.first).getExpressions().get(0).getValue();
                    return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(exp, p.first.getInputs().get(0));
                } else {
                    Mutable<ILogicalOperator> src = new MutableObject<ILogicalOperator>();

                    Pair<ILogicalOperator, LogicalVariable> p = expr.accept(this, src);

                    if (((AbstractLogicalOperator) p.first).getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                        // src.setOperator(topOp.getOperator());
                        Mutable<ILogicalOperator> top2 = new MutableObject<ILogicalOperator>(p.first);
                        return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(
                                new VariableReferenceExpression(p.second), top2);
                    } else {
                        SubplanOperator s = new SubplanOperator();
                        s.getInputs().add(topOp);
                        src.setValue(new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(s)));
                        Mutable<ILogicalOperator> planRoot = new MutableObject<ILogicalOperator>(p.first);
                        s.setRootOp(planRoot);
                        return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(
                                new VariableReferenceExpression(p.second), new MutableObject<ILogicalOperator>(s));
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
        List<OperatorType> ops = op.getOpList();
        int nOps = ops.size();

        List<Expression> exprs = op.getExprList();

        Mutable<ILogicalOperator> topOp = tupSource;

        OperatorType opLogical = ops.get(0);
        AbstractFunctionCallExpression f = createFunctionCallExpressionForBuiltinOperator(opLogical);

        for (int i = 0; i <= nOps; i++) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(exprs.get(i), topOp);
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

    private static boolean expressionNeedsNoNesting(Expression expr) {
        Kind k = expr.getKind();
        return k == Kind.LITERAL_EXPRESSION || k == Kind.LIST_CONSTRUCTOR_EXPRESSION
                || k == Kind.RECORD_CONSTRUCTOR_EXPRESSION || k == Kind.VARIABLE_EXPRESSION || k == Kind.CALL_EXPRESSION
                || k == Kind.OP_EXPRESSION || k == Kind.FIELD_ACCESSOR_EXPRESSION || k == Kind.INDEX_ACCESSOR_EXPRESSION
                || k == Kind.UNARY_EXPRESSION;
    }

    private <T> ArrayList<T> mkSingletonArrayList(T item) {
        ArrayList<T> array = new ArrayList<T>(1);
        array.add(item);
        return array;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(TypeDecl td, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(RecordTypeDefinition tre, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(TypeReferenceExpression tre, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(NodegroupDecl ngd, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(LoadStatement stmtLoad, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(DropStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CreateIndexStatement cis, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(OrderedListTypeDefinition olte, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(UnorderedListTypeDefinition ulte,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitMetaVariableClause(MetaVariableClause mc,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        return new Pair<ILogicalOperator, LogicalVariable>(metaScopeOp.get(mc.getVar()), null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitJoinClause(JoinClause jc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        // Pair<ILogicalOperator, LogicalVariable> leftSide =
        // jc.getLeftExpr().accept(this, tupSource);
        Mutable<ILogicalOperator> opRef = tupSource;
        Pair<ILogicalOperator, LogicalVariable> leftSide = null;
        for (Clause c : jc.getLeftClauses()) {
            leftSide = c.accept(this, opRef);
            opRef = new MutableObject<ILogicalOperator>(leftSide.first);
        }

        // Pair<ILogicalOperator, LogicalVariable> rightSide =
        // jc.getRightExpr().accept(this, tupSource);
        opRef = tupSource;
        Pair<ILogicalOperator, LogicalVariable> rightSide = null;
        for (Clause c : jc.getRightClauses()) {
            rightSide = c.accept(this, opRef);
            opRef = new MutableObject<ILogicalOperator>(rightSide.first);
        }

        Pair<ILogicalExpression, Mutable<ILogicalOperator>> whereCond = aqlExprToAlgExpression(jc.getWhereExpr(),
                tupSource);

        AbstractBinaryJoinOperator join;
        switch (jc.getKind()) {
            case INNER: {
                join = new InnerJoinOperator(new MutableObject<ILogicalExpression>(whereCond.first));
                break;
            }
            case LEFT_OUTER: {
                join = new LeftOuterJoinOperator(new MutableObject<ILogicalExpression>(whereCond.first));
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
        join.getInputs().add(new MutableObject<ILogicalOperator>(leftSide.first));
        join.getInputs().add(new MutableObject<ILogicalOperator>(rightSide.first));
        return new Pair<ILogicalOperator, LogicalVariable>(join, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitMetaVariableExpr(MetaVariableExpr me,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        LogicalVariable var = context.newVar();
        AssignOperator a = new AssignOperator(var,
                new MutableObject<ILogicalExpression>(metaScopeExp.getVariableReferenceExpression(me.getVar())));
        a.getInputs().add(tupSource);
        return new Pair<ILogicalOperator, LogicalVariable>(a, var);
    }

    public void addOperatorToMetaScope(Identifier id, ILogicalOperator op) {
        metaScopeOp.put(id, op);
    }

    public void addVariableToMetaScope(Identifier id, LogicalVariable var) {
        metaScopeExp.put(id, var);
    }

    private ILogicalExpression makeUnnestExpression(ILogicalExpression expr) {
        switch (expr.getExpressionTag()) {
            case VARIABLE: {
                return new UnnestingFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION),
                        new MutableObject<ILogicalExpression>(expr));
            }
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                if (fce.getKind() == FunctionKind.UNNEST) {
                    return expr;
                } else {
                    return new UnnestingFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION),
                            new MutableObject<ILogicalExpression>(expr));
                }
            }
            default: {
                return expr;
            }
        }
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(InsertStatement insert, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(DeleteStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(UpdateStatement update, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(UpdateClause del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(DataverseDecl dv, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(DatasetDecl dd, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SetStatement ss, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(WriteStatement ws, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CreateDataverseStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(IndexDropStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(NodeGroupDropStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(DataverseDropStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(TypeDropStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(DisconnectFeedStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
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
    public Pair<ILogicalOperator, LogicalVariable> visit(FunctionDropStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(ConnectFeedStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FeedDropStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CompactStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CreatePrimaryFeedStatement del, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CreateSecondaryFeedStatement del,
            Mutable<ILogicalOperator> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CreateFeedPolicyStatement cfps, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FeedPolicyDropStatement dfs, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }
}
