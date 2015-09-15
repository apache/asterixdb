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
package org.apache.asterix.aql.rewrites;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.aql.base.Clause;
import org.apache.asterix.aql.base.Expression;
import org.apache.asterix.aql.base.Expression.Kind;
import org.apache.asterix.aql.expression.CallExpr;
import org.apache.asterix.aql.expression.CompactStatement;
import org.apache.asterix.aql.expression.ConnectFeedStatement;
import org.apache.asterix.aql.expression.CreateDataverseStatement;
import org.apache.asterix.aql.expression.CreateFeedPolicyStatement;
import org.apache.asterix.aql.expression.CreateFunctionStatement;
import org.apache.asterix.aql.expression.CreateIndexStatement;
import org.apache.asterix.aql.expression.CreatePrimaryFeedStatement;
import org.apache.asterix.aql.expression.CreateSecondaryFeedStatement;
import org.apache.asterix.aql.expression.DatasetDecl;
import org.apache.asterix.aql.expression.DataverseDecl;
import org.apache.asterix.aql.expression.DataverseDropStatement;
import org.apache.asterix.aql.expression.DeleteStatement;
import org.apache.asterix.aql.expression.DisconnectFeedStatement;
import org.apache.asterix.aql.expression.DistinctClause;
import org.apache.asterix.aql.expression.DropStatement;
import org.apache.asterix.aql.expression.FLWOGRExpression;
import org.apache.asterix.aql.expression.FeedDropStatement;
import org.apache.asterix.aql.expression.FeedPolicyDropStatement;
import org.apache.asterix.aql.expression.FieldAccessor;
import org.apache.asterix.aql.expression.FieldBinding;
import org.apache.asterix.aql.expression.ForClause;
import org.apache.asterix.aql.expression.FunctionDecl;
import org.apache.asterix.aql.expression.FunctionDropStatement;
import org.apache.asterix.aql.expression.GbyVariableExpressionPair;
import org.apache.asterix.aql.expression.GroupbyClause;
import org.apache.asterix.aql.expression.IfExpr;
import org.apache.asterix.aql.expression.IndexAccessor;
import org.apache.asterix.aql.expression.IndexDropStatement;
import org.apache.asterix.aql.expression.InsertStatement;
import org.apache.asterix.aql.expression.LetClause;
import org.apache.asterix.aql.expression.LimitClause;
import org.apache.asterix.aql.expression.ListConstructor;
import org.apache.asterix.aql.expression.LiteralExpr;
import org.apache.asterix.aql.expression.LoadStatement;
import org.apache.asterix.aql.expression.NodeGroupDropStatement;
import org.apache.asterix.aql.expression.NodegroupDecl;
import org.apache.asterix.aql.expression.OperatorExpr;
import org.apache.asterix.aql.expression.OrderbyClause;
import org.apache.asterix.aql.expression.OrderedListTypeDefinition;
import org.apache.asterix.aql.expression.QuantifiedExpression;
import org.apache.asterix.aql.expression.QuantifiedPair;
import org.apache.asterix.aql.expression.Query;
import org.apache.asterix.aql.expression.RecordConstructor;
import org.apache.asterix.aql.expression.RecordTypeDefinition;
import org.apache.asterix.aql.expression.SetStatement;
import org.apache.asterix.aql.expression.TypeDecl;
import org.apache.asterix.aql.expression.TypeDropStatement;
import org.apache.asterix.aql.expression.TypeReferenceExpression;
import org.apache.asterix.aql.expression.UnaryExpr;
import org.apache.asterix.aql.expression.UnionExpr;
import org.apache.asterix.aql.expression.UnorderedListTypeDefinition;
import org.apache.asterix.aql.expression.UpdateClause;
import org.apache.asterix.aql.expression.UpdateStatement;
import org.apache.asterix.aql.expression.VarIdentifier;
import org.apache.asterix.aql.expression.VariableExpr;
import org.apache.asterix.aql.expression.WhereClause;
import org.apache.asterix.aql.expression.WriteStatement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.util.FunctionUtils;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.AsterixFunction;

public final class AqlRewriter {

    private final Query topExpr;
    private final List<FunctionDecl> declaredFunctions;
    private final AqlRewritingContext context;
    private final MetadataTransactionContext mdTxnCtx;
    private final AqlMetadataProvider metadataProvider;

    private enum DfsColor {
        WHITE,
        GRAY,
        BLACK
    }

    public AqlRewriter(List<FunctionDecl> declaredFunctions, Query topExpr, AqlMetadataProvider metadataProvider) {
        this.topExpr = topExpr;
        context = new AqlRewritingContext(topExpr.getVarCounter());
        this.declaredFunctions = declaredFunctions;
        this.mdTxnCtx = metadataProvider.getMetadataTxnContext();
        this.metadataProvider = metadataProvider;
    }

    public Query getExpr() {
        return topExpr;
    }

    public int getVarCounter() {
        return context.getVarCounter();
    }

    public void rewrite() throws AsterixException {
        wrapInLets();
        inlineDeclaredUdfs();
    }

    private void wrapInLets() {
        // If the top expression of the main statement is not a FLWOR, it wraps
        // it into a let clause.
        if (topExpr == null) {
            return;
        }
        Expression body = topExpr.getBody();
        if (body.getKind() != Kind.FLWOGR_EXPRESSION) {
            VarIdentifier var = context.newVariable();
            VariableExpr v = new VariableExpr(var);
            LetClause c1 = new LetClause(v, body);
            ArrayList<Clause> clauseList = new ArrayList<Clause>(1);
            clauseList.add(c1);
            FLWOGRExpression newBody = new FLWOGRExpression(clauseList, new VariableExpr(var));
            topExpr.setBody(newBody);
        }
    }

    private void inlineDeclaredUdfs() throws AsterixException {
        if (topExpr == null) {
            return;
        }
        List<FunctionSignature> funIds = new ArrayList<FunctionSignature>();
        for (FunctionDecl fdecl : declaredFunctions) {
            funIds.add(fdecl.getSignature());
        }

        List<FunctionDecl> otherFDecls = new ArrayList<FunctionDecl>();
        buildOtherUdfs(topExpr.getBody(), otherFDecls, funIds);
        declaredFunctions.addAll(otherFDecls);
        if (!declaredFunctions.isEmpty()) {
            InlineUdfsVisitor visitor = new InlineUdfsVisitor(context);
            while (topExpr.accept(visitor, declaredFunctions)) {
                // loop until no more changes
            }
        }
        declaredFunctions.removeAll(otherFDecls);
    }

    private void buildOtherUdfs(Expression expression, List<FunctionDecl> functionDecls,
            List<FunctionSignature> declaredFunctions) throws AsterixException {
        if (expression == null) {
            return;
        }
        String value = metadataProvider.getConfig().get(FunctionUtils.IMPORT_PRIVATE_FUNCTIONS);
        boolean includePrivateFunctions = (value != null) ? Boolean.valueOf(value.toLowerCase()) : false;
        Set<FunctionSignature> functionCalls = getFunctionCalls(expression);
        for (FunctionSignature signature : functionCalls) {

            if (declaredFunctions != null && declaredFunctions.contains(signature)) {
                continue;
            }

            Function function = lookupUserDefinedFunctionDecl(signature);
            if (function == null) {
                if (AsterixBuiltinFunctions.isBuiltinCompilerFunction(signature, includePrivateFunctions)) {
                    continue;
                }
                StringBuilder messageBuilder = new StringBuilder();
                if (functionDecls.size() > 0) {
                    messageBuilder.append(" function " + functionDecls.get(functionDecls.size() - 1).getSignature()
                            + " depends upon function " + signature + " which is undefined");
                } else {
                    messageBuilder.append(" function " + signature + " is undefined ");
                }
                throw new AsterixException(messageBuilder.toString());
            }

            if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)) {
                FunctionDecl functionDecl = FunctionUtils.getFunctionDecl(function);
                if (functionDecl != null) {
                    if (functionDecls.contains(functionDecl)) {
                        throw new AsterixException("ERROR:Recursive invocation "
                                + functionDecls.get(functionDecls.size() - 1).getSignature() + " <==> "
                                + functionDecl.getSignature());
                    }
                    functionDecls.add(functionDecl);
                    buildOtherUdfs(functionDecl.getFuncBody(), functionDecls, declaredFunctions);
                }
            }
        }

    }

    private Function lookupUserDefinedFunctionDecl(FunctionSignature signature) throws AsterixException {
        if (signature.getNamespace() == null) {
            return null;
        }
        return MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
    }

    private Set<FunctionSignature> getFunctionCalls(Expression expression) throws AsterixException {
        Map<AsterixFunction, DfsColor> color = new HashMap<AsterixFunction, DfsColor>();
        Map<AsterixFunction, List<AsterixFunction>> arcs = new HashMap<AsterixFunction, List<AsterixFunction>>();
        GatherFunctionCalls gfc = new GatherFunctionCalls();
        expression.accept(gfc, null);
        return gfc.getCalls();
    }

    private static class GatherFunctionCalls implements IAqlExpressionVisitor<Void, Void> {

        private final Set<FunctionSignature> calls = new HashSet<FunctionSignature>();

        public GatherFunctionCalls() {
        }

        @Override
        public Void visitCallExpr(CallExpr pf, Void arg) throws AsterixException {
            calls.add(pf.getFunctionSignature());
            for (Expression e : pf.getExprList()) {
                e.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visitCreateIndexStatement(CreateIndexStatement cis, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitDataverseDecl(DataverseDecl dv, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitDeleteStatement(DeleteStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitDistinctClause(DistinctClause dc, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitDropStatement(DropStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitDatasetDecl(DatasetDecl dd, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitFieldAccessor(FieldAccessor fa, Void arg) throws AsterixException {
            fa.getExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visitFlworExpression(FLWOGRExpression flwor, Void arg) throws AsterixException {
            for (Clause c : flwor.getClauseList()) {
                c.accept(this, arg);
            }
            flwor.getReturnExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visitForClause(ForClause fc, Void arg) throws AsterixException {
            fc.getInExpr().accept(this, arg);
            if (fc.getPosVarExpr() != null) {
                fc.getPosVarExpr().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visitFunctionDecl(FunctionDecl fd, Void arg) throws AsterixException {
            fd.getFuncBody().accept(this, arg);
            return null;
        }

        @Override
        public Void visitGroupbyClause(GroupbyClause gc, Void arg) throws AsterixException {
            for (GbyVariableExpressionPair p : gc.getGbyPairList()) {
                p.getExpr().accept(this, arg);
            }
            for (GbyVariableExpressionPair p : gc.getDecorPairList()) {
                p.getExpr().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visitIfExpr(IfExpr ifexpr, Void arg) throws AsterixException {
            ifexpr.getCondExpr().accept(this, arg);
            ifexpr.getThenExpr().accept(this, arg);
            ifexpr.getElseExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visitIndexAccessor(IndexAccessor ia, Void arg) throws AsterixException {
            ia.getExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visitInsertStatement(InsertStatement insert, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitLetClause(LetClause lc, Void arg) throws AsterixException {
            lc.getBindingExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visitLimitClause(LimitClause lc, Void arg) throws AsterixException {
            lc.getLimitExpr().accept(this, arg);
            if (lc.getOffset() != null) {
                lc.getOffset().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visitListConstructor(ListConstructor lc, Void arg) throws AsterixException {
            for (Expression e : lc.getExprList()) {
                e.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visitLiteralExpr(LiteralExpr l, Void arg) throws AsterixException {
            // do nothing
            return null;
        }

        @Override
        public Void visitLoadStatement(LoadStatement stmtLoad, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitNodegroupDecl(NodegroupDecl ngd, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitOperatorExpr(OperatorExpr op, Void arg) throws AsterixException {
            for (Expression e : op.getExprList()) {
                e.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visitOrderbyClause(OrderbyClause oc, Void arg) throws AsterixException {
            for (Expression e : oc.getOrderbyList()) {
                e.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visitOrderedListTypeDefiniton(OrderedListTypeDefinition olte, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitQuantifiedExpression(QuantifiedExpression qe, Void arg) throws AsterixException {
            for (QuantifiedPair qp : qe.getQuantifiedList()) {
                qp.getExpr().accept(this, arg);
            }
            qe.getSatisfiesExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visitQuery(Query q, Void arg) throws AsterixException {
            q.getBody().accept(this, arg);
            return null;
        }

        @Override
        public Void visitRecordConstructor(RecordConstructor rc, Void arg) throws AsterixException {
            for (FieldBinding fb : rc.getFbList()) {
                fb.getLeftExpr().accept(this, arg);
                fb.getRightExpr().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visitRecordTypeDefiniton(RecordTypeDefinition tre, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitSetStatement(SetStatement ss, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitTypeDecl(TypeDecl td, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitTypeReferenceExpression(TypeReferenceExpression tre, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitUnaryExpr(UnaryExpr u, Void arg) throws AsterixException {
            u.getExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visitUnionExpr(UnionExpr u, Void arg) throws AsterixException {
            for (Expression e : u.getExprs()) {
                e.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visitUnorderedListTypeDefiniton(UnorderedListTypeDefinition ulte, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitUpdateClause(UpdateClause del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitUpdateStatement(UpdateStatement update, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitVariableExpr(VariableExpr v, Void arg) throws AsterixException {
            // do nothing
            return null;
        }

        @Override
        public Void visitWhereClause(WhereClause wc, Void arg) throws AsterixException {
            wc.getWhereExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visitWriteStatement(WriteStatement ws, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        public Set<FunctionSignature> getCalls() {
            return calls;
        }

        @Override
        public Void visitCreateDataverseStatement(CreateDataverseStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitIndexDropStatement(IndexDropStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitNodeGroupDropStatement(NodeGroupDropStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitDataverseDropStatement(DataverseDropStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitTypeDropStatement(TypeDropStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitDisconnectFeedStatement(DisconnectFeedStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visit(CreateFunctionStatement cfs, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitFunctionDropStatement(FunctionDropStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitCreatePrimaryFeedStatement(CreatePrimaryFeedStatement del, Void arg) throws AsterixException {
            return null;
        }

        @Override
        public Void visitCreateSecondaryFeedStatement(CreateSecondaryFeedStatement del, Void arg)
                throws AsterixException {
            return null;
        }

        @Override
        public Void visitConnectFeedStatement(ConnectFeedStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitDropFeedStatement(FeedDropStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitCompactStatement(CompactStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }
        
        @Override
        public Void visitCreateFeedPolicyStatement(CreateFeedPolicyStatement cfps, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitDropFeedPolicyStatement(FeedPolicyDropStatement dfs, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

    }
}
