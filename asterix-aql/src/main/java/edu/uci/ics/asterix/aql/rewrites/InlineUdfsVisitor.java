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
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Expression.Kind;
import edu.uci.ics.asterix.aql.base.IAqlExpression;
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
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

public class InlineUdfsVisitor implements IAqlExpressionVisitor<Boolean, List<FunctionDecl>> {

    private final AqlRewritingContext context;
    private final CloneAndSubstituteVariablesVisitor cloneVisitor;

    public InlineUdfsVisitor(AqlRewritingContext context) {
        this.context = context;
        this.cloneVisitor = new CloneAndSubstituteVariablesVisitor(context);
    }

    @Override
    public Boolean visitQuery(Query q, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(q.getBody(), arg);
        q.setBody(p.second);
        return p.first;
    }

    @Override
    public Boolean visitFunctionDecl(FunctionDecl fd, List<FunctionDecl> arg) throws AsterixException {
        // Careful, we should only do this after analyzing the graph of function
        // calls.
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fd.getFuncBody(), arg);
        fd.setFuncBody(p.second);
        return p.first;
    }

    @Override
    public Boolean visitListConstructor(ListConstructor lc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, ArrayList<Expression>> p = newExprList(lc.getExprList(), arg);
        lc.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visitRecordConstructor(RecordConstructor rc, List<FunctionDecl> arg) throws AsterixException {
        boolean changed = false;
        for (FieldBinding b : rc.getFbList()) {
            Pair<Boolean, Expression> leftExprInlined = inlineUdfsInExpr(b.getLeftExpr(), arg);
            b.setLeftExpr(leftExprInlined.second);
            changed = changed | leftExprInlined.first;
            Pair<Boolean, Expression> rightExprInlined = inlineUdfsInExpr(b.getRightExpr(), arg);
            b.setRightExpr(rightExprInlined.second);
            changed = changed | rightExprInlined.first;

            /*
            if (b.getLeftExpr().accept(this, arg)) {
                changed = true;
            }
            if (b.getRightExpr().accept(this, arg)) {
                changed = true;
            }*/
        }
        return changed;
    }

    @Override
    public Boolean visitCallExpr(CallExpr pf, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, ArrayList<Expression>> p = newExprList(pf.getExprList(), arg);
        pf.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visitOperatorExpr(OperatorExpr ifbo, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, ArrayList<Expression>> p = newExprList(ifbo.getExprList(), arg);
        ifbo.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visitFieldAccessor(FieldAccessor fa, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fa.getExpr(), arg);
        fa.setExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visitIndexAccessor(IndexAccessor fa, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fa.getExpr(), arg);
        fa.setExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visitIfExpr(IfExpr ifexpr, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p1 = inlineUdfsInExpr(ifexpr.getCondExpr(), arg);
        ifexpr.setCondExpr(p1.second);
        Pair<Boolean, Expression> p2 = inlineUdfsInExpr(ifexpr.getThenExpr(), arg);
        ifexpr.setThenExpr(p2.second);
        Pair<Boolean, Expression> p3 = inlineUdfsInExpr(ifexpr.getElseExpr(), arg);
        ifexpr.setElseExpr(p3.second);
        return p1.first || p2.first || p3.first;
    }

    @Override
    public Boolean visitFlworExpression(FLWOGRExpression flwor, List<FunctionDecl> arg) throws AsterixException {
        boolean changed = false;
        for (Clause c : flwor.getClauseList()) {
            if (c.accept(this, arg)) {
                changed = true;
            }
        }
        Pair<Boolean, Expression> p = inlineUdfsInExpr(flwor.getReturnExpr(), arg);
        flwor.setReturnExpr(p.second);
        return changed || p.first;
    }

    @Override
    public Boolean visitQuantifiedExpression(QuantifiedExpression qe, List<FunctionDecl> arg) throws AsterixException {
        boolean changed = false;
        for (QuantifiedPair t : qe.getQuantifiedList()) {
            Pair<Boolean, Expression> p = inlineUdfsInExpr(t.getExpr(), arg);
            t.setExpr(p.second);
            if (p.first) {
                changed = true;
            }
        }
        Pair<Boolean, Expression> p2 = inlineUdfsInExpr(qe.getSatisfiesExpr(), arg);
        qe.setSatisfiesExpr(p2.second);
        return changed || p2.first;
    }

    @Override
    public Boolean visitForClause(ForClause fc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fc.getInExpr(), arg);
        fc.setInExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visitLetClause(LetClause lc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(lc.getBindingExpr(), arg);
        lc.setBindingExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visitWhereClause(WhereClause wc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(wc.getWhereExpr(), arg);
        wc.setWhereExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visitOrderbyClause(OrderbyClause oc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, ArrayList<Expression>> p = newExprList(oc.getOrderbyList(), arg);
        oc.setOrderbyList(p.second);
        return p.first;
    }

    @Override
    public Boolean visitGroupbyClause(GroupbyClause gc, List<FunctionDecl> arg) throws AsterixException {
        boolean changed = false;
        for (GbyVariableExpressionPair p : gc.getGbyPairList()) {
            Pair<Boolean, Expression> be = inlineUdfsInExpr(p.getExpr(), arg);
            p.setExpr(be.second);
            if (be.first) {
                changed = true;
            }
        }
        for (GbyVariableExpressionPair p : gc.getDecorPairList()) {
            Pair<Boolean, Expression> be = inlineUdfsInExpr(p.getExpr(), arg);
            p.setExpr(be.second);
            if (be.first) {
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public Boolean visitLimitClause(LimitClause lc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p1 = inlineUdfsInExpr(lc.getLimitExpr(), arg);
        lc.setLimitExpr(p1.second);
        boolean changed = p1.first;
        if (lc.getOffset() != null) {
            Pair<Boolean, Expression> p2 = inlineUdfsInExpr(lc.getOffset(), arg);
            lc.setOffset(p2.second);
            changed = changed || p2.first;
        }
        return changed;
    }

    @Override
    public Boolean visitUnaryExpr(UnaryExpr u, List<FunctionDecl> arg) throws AsterixException {
        return u.getExpr().accept(this, arg);
    }

    @Override
    public Boolean visitUnionExpr(UnionExpr u, List<FunctionDecl> fds) throws AsterixException {
        Pair<Boolean, ArrayList<Expression>> p = newExprList(u.getExprs(), fds);
        u.setExprs(p.second);
        return p.first;
    }

    @Override
    public Boolean visitDistinctClause(DistinctClause dc, List<FunctionDecl> arg) throws AsterixException {
        boolean changed = false;
        for (Expression expr : dc.getDistinctByExpr()) {
            changed = expr.accept(this, arg);
        }
        return changed;
    }

    @Override
    public Boolean visitVariableExpr(VariableExpr v, List<FunctionDecl> arg) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visitLiteralExpr(LiteralExpr l, List<FunctionDecl> arg) throws AsterixException {
        return false;
    }

    private Pair<Boolean, Expression> inlineUdfsInExpr(Expression expr, List<FunctionDecl> arg) throws AsterixException {
        if (expr.getKind() != Kind.CALL_EXPRESSION) {
            boolean r = expr.accept(this, arg);
            return new Pair<Boolean, Expression>(r, expr);
        } else {
            CallExpr f = (CallExpr) expr;
            FunctionDecl implem = findFuncDeclaration(f.getFunctionSignature(), arg);
            if (implem == null) {
                boolean r = expr.accept(this, arg);
                return new Pair<Boolean, Expression>(r, expr);
            } else { // it's one of the functions we want to inline
                List<Clause> clauses = new ArrayList<Clause>();
                Iterator<VarIdentifier> paramIter = implem.getParamList().iterator();
                // List<VariableExpr> effectiveArgs = new
                // ArrayList<VariableExpr>(f.getExprList().size());
                List<VariableSubstitution> subts = new ArrayList<VariableSubstitution>(f.getExprList().size());
                for (Expression e : f.getExprList()) {
                    VarIdentifier param = paramIter.next();
                    // Obs: we could do smth about passing also literals, or let
                    // variable inlining to take care of this.
                    if (e.getKind() == Kind.VARIABLE_EXPRESSION) {
                        subts.add(new VariableSubstitution(param, ((VariableExpr) e).getVar()));
                    } else {
                        VarIdentifier newV = context.newVariable();
                        Pair<IAqlExpression, List<VariableSubstitution>> p1 = e.accept(cloneVisitor,
                                new ArrayList<VariableSubstitution>());
                        LetClause c = new LetClause(new VariableExpr(newV), (Expression) p1.first);
                        clauses.add(c);
                        subts.add(new VariableSubstitution(param, newV));
                    }
                }
                Pair<IAqlExpression, List<VariableSubstitution>> p2 = implem.getFuncBody().accept(cloneVisitor, subts);
                Expression resExpr;
                if (clauses.isEmpty()) {
                    resExpr = (Expression) p2.first;
                } else {
                    resExpr = new FLWOGRExpression(clauses, (Expression) p2.first);
                }
                return new Pair<Boolean, Expression>(true, resExpr);
            }
        }
    }

    private Pair<Boolean, ArrayList<Expression>> newExprList(List<Expression> exprList, List<FunctionDecl> fds)
            throws AsterixException {
        ArrayList<Expression> newList = new ArrayList<Expression>();
        boolean changed = false;
        for (Expression e : exprList) {
            Pair<Boolean, Expression> p = inlineUdfsInExpr(e, fds);
            newList.add(p.second);
            if (p.first) {
                changed = true;
            }
        }
        return new Pair<Boolean, ArrayList<Expression>>(changed, newList);
    }

    private static FunctionDecl findFuncDeclaration(FunctionSignature fid, List<FunctionDecl> sequence) {
        for (FunctionDecl f : sequence) {
            if (f.getSignature().equals(fid)) {
                return f;
            }
        }
        return null;
    }

    @Override
    public Boolean visitCreateIndexStatement(CreateIndexStatement cis, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitDataverseDecl(DataverseDecl dv, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitDeleteStatement(DeleteStatement del, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitDropStatement(DropStatement del, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitDatasetDecl(DatasetDecl dd, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitInsertStatement(InsertStatement insert, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitLoadFromFileStatement(LoadFromFileStatement stmtLoad, List<FunctionDecl> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitNodegroupDecl(NodegroupDecl ngd, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitOrderedListTypeDefiniton(OrderedListTypeDefinition olte, List<FunctionDecl> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitRecordTypeDefiniton(RecordTypeDefinition tre, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitSetStatement(SetStatement ss, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitTypeDecl(TypeDecl td, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitTypeReferenceExpression(TypeReferenceExpression tre, List<FunctionDecl> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitUnorderedListTypeDefiniton(UnorderedListTypeDefinition ulte, List<FunctionDecl> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitUpdateClause(UpdateClause del, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitUpdateStatement(UpdateStatement update, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitWriteStatement(WriteStatement ws, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitCreateDataverseStatement(CreateDataverseStatement del, List<FunctionDecl> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitIndexDropStatement(IndexDropStatement del, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitNodeGroupDropStatement(NodeGroupDropStatement del, List<FunctionDecl> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitDataverseDropStatement(DataverseDropStatement del, List<FunctionDecl> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitTypeDropStatement(TypeDropStatement del, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitControlFeedStatement(ControlFeedStatement del, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visit(CreateFunctionStatement cfs, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitFunctionDropStatement(FunctionDropStatement del, List<FunctionDecl> arg)
            throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitBeginFeedStatement(BeginFeedStatement bf, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean visitCompactStatement(CompactStatement del, List<FunctionDecl> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }
}
