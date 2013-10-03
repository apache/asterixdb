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
package edu.uci.ics.asterix.aql.rewrites;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import edu.uci.ics.asterix.aql.expression.LiteralExpr;
import edu.uci.ics.asterix.aql.expression.LoadFromFileStatement;
import edu.uci.ics.asterix.aql.expression.NodeGroupDropStatement;
import edu.uci.ics.asterix.aql.expression.NodegroupDecl;
import edu.uci.ics.asterix.aql.expression.OperatorExpr;
import edu.uci.ics.asterix.aql.expression.OrderbyClause;
import edu.uci.ics.asterix.aql.expression.OrderedListTypeDefinition;
import edu.uci.ics.asterix.aql.expression.QuantifiedExpression;
import edu.uci.ics.asterix.aql.expression.QuantifiedPair;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.RecordConstructor;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition;
import edu.uci.ics.asterix.aql.expression.SetStatement;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.TypeDropStatement;
import edu.uci.ics.asterix.aql.expression.TypeReferenceExpression;
import edu.uci.ics.asterix.aql.expression.UnaryExpr;
import edu.uci.ics.asterix.aql.expression.UnionExpr;
import edu.uci.ics.asterix.aql.expression.UnorderedListTypeDefinition;
import edu.uci.ics.asterix.aql.expression.UpdateClause;
import edu.uci.ics.asterix.aql.expression.UpdateStatement;
import edu.uci.ics.asterix.aql.expression.VarIdentifier;
import edu.uci.ics.asterix.aql.expression.VariableExpr;
import edu.uci.ics.asterix.aql.expression.WhereClause;
import edu.uci.ics.asterix.aql.expression.WriteStatement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.AsterixFunction;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

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

            FunctionDecl functionDecl = lookupUserDefinedFunctionDecl(signature);
            if (functionDecl != null) {
                if (functionDecls.contains(functionDecl)) {
                    throw new AsterixException(" Detected recursvity!");
                } else {
                    functionDecls.add(functionDecl);
                    buildOtherUdfs(functionDecl.getFuncBody(), functionDecls, declaredFunctions);
                }
            } else {
                if (AsterixBuiltinFunctions.isBuiltinCompilerFunction(signature, includePrivateFunctions)) {
                    continue;
                } else {
                    throw new AsterixException(" unknown function " + signature);
                }
            }
        }
    }

    private FunctionDecl lookupUserDefinedFunctionDecl(FunctionSignature signature) throws AsterixException {
        if (signature.getNamespace() == null) {
            return null;
        }
        Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
        if (function == null) {
            return null;
        }
        return FunctionUtils.getFunctionDecl(function);

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
        public Void visitLoadFromFileStatement(LoadFromFileStatement stmtLoad, Void arg) throws AsterixException {
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
        public Void visitControlFeedStatement(ControlFeedStatement del, Void arg) throws AsterixException {
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
        public Void visitBeginFeedStatement(BeginFeedStatement bf, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Void visitCompactStatement(CompactStatement del, Void arg) throws AsterixException {
            // TODO Auto-generated method stub
            return null;
        }

    }
}
