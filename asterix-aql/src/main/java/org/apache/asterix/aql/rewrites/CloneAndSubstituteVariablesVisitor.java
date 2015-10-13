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
import java.util.LinkedList;
import java.util.List;

import org.apache.asterix.aql.base.Clause;
import org.apache.asterix.aql.base.Expression;
import org.apache.asterix.aql.base.IAqlExpression;
import org.apache.asterix.aql.expression.CallExpr;
import org.apache.asterix.aql.expression.ChannelDropStatement;
import org.apache.asterix.aql.expression.ChannelSubscribeStatement;
import org.apache.asterix.aql.expression.ChannelUnsubscribeStatement;
import org.apache.asterix.aql.expression.CompactStatement;
import org.apache.asterix.aql.expression.ConnectFeedStatement;
import org.apache.asterix.aql.expression.CreateChannelStatement;
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
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class CloneAndSubstituteVariablesVisitor implements
        IAqlExpressionVisitor<Pair<IAqlExpression, List<VariableSubstitution>>, List<VariableSubstitution>> {

    private AqlRewritingContext context;

    public CloneAndSubstituteVariablesVisitor(AqlRewritingContext context) {
        this.context = context;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitFieldAccessor(FieldAccessor fa,
            List<VariableSubstitution> arg) throws AsterixException {
        Pair<IAqlExpression, List<VariableSubstitution>> p = fa.getExpr().accept(this, arg);
        FieldAccessor newF = new FieldAccessor((Expression) p.first, fa.getIdent());
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newF, p.second);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitFlworExpression(FLWOGRExpression flwor,
            List<VariableSubstitution> arg) throws AsterixException {
        List<Clause> newClauses = new ArrayList<Clause>(flwor.getClauseList().size());
        List<VariableSubstitution> ongoing = arg;
        for (Clause c : flwor.getClauseList()) {
            Pair<IAqlExpression, List<VariableSubstitution>> p1 = c.accept(this, ongoing);
            ongoing = p1.second;
            newClauses.add((Clause) p1.first);
        }
        Pair<IAqlExpression, List<VariableSubstitution>> p2 = flwor.getReturnExpr().accept(this, ongoing);
        Expression newReturnExpr = (Expression) p2.first;
        FLWOGRExpression newFlwor = new FLWOGRExpression(newClauses, newReturnExpr);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newFlwor, p2.second);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitForClause(ForClause fc, List<VariableSubstitution> arg)
            throws AsterixException {
        Pair<IAqlExpression, List<VariableSubstitution>> p1 = fc.getInExpr().accept(this, arg);
        VarIdentifier vi = fc.getVarExpr().getVar();
        // we need new variables
        VarIdentifier newVar = context.mapOldId(vi.getId(), vi.getValue());

        VariableSubstitution vs = findVarSubst(arg, vi);
        List<VariableSubstitution> newSubs;
        if (vs == null) {
            newSubs = arg;
        } else {
            // This for clause is overriding a binding, so we don't subst. that
            // one anymore.
            newSubs = eliminateSubstFromList(vi, arg);
        }

        VariableExpr newVe = new VariableExpr(newVar);
        ForClause newFor = new ForClause(newVe, (Expression) p1.first);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newFor, newSubs);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitLetClause(LetClause lc, List<VariableSubstitution> arg)
            throws AsterixException {
        Pair<IAqlExpression, List<VariableSubstitution>> p1 = lc.getBindingExpr().accept(this, arg);
        VarIdentifier vi = lc.getVarExpr().getVar();
        VarIdentifier newVar = context.mapOldId(vi.getId(), vi.getValue());

        VariableSubstitution vs = findVarSubst(arg, vi);
        List<VariableSubstitution> newSubs;
        if (vs == null) {
            newSubs = arg;
        } else {
            newSubs = eliminateSubstFromList(vi, arg);
        }

        VariableExpr newVe = new VariableExpr(newVar);
        LetClause newLet = new LetClause(newVe, (Expression) p1.first);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newLet, newSubs);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitGroupbyClause(GroupbyClause gc,
            List<VariableSubstitution> arg) throws AsterixException {
        List<VariableSubstitution> newSubs = arg;
        List<GbyVariableExpressionPair> newGbyList = substInVarExprPair(gc.getGbyPairList(), arg, newSubs);
        List<GbyVariableExpressionPair> newDecorList = substInVarExprPair(gc.getDecorPairList(), arg, newSubs);
        List<VariableExpr> wList = new LinkedList<VariableExpr>();
        for (VariableExpr w : gc.getWithVarList()) {
            VarIdentifier newVar = context.getRewrittenVar(w.getVar().getId());
            wList.add(new VariableExpr(newVar));
        }
        GroupbyClause newGroup = new GroupbyClause(newGbyList, newDecorList, wList, gc.hasHashGroupByHint());
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newGroup, newSubs);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitQuantifiedExpression(QuantifiedExpression qe,
            List<VariableSubstitution> arg) throws AsterixException {
        List<QuantifiedPair> oldPairs = qe.getQuantifiedList();
        List<QuantifiedPair> newPairs = new ArrayList<QuantifiedPair>(oldPairs.size());
        List<VarIdentifier> newVis = new LinkedList<VarIdentifier>();
        List<VariableSubstitution> newSubs = arg;
        for (QuantifiedPair t : oldPairs) {
            VarIdentifier newVar = context.mapOldVarIdentifier(t.getVarExpr().getVar());
            newVis.add(newVar);
            newSubs = eliminateSubstFromList(newVar, newSubs);
            Pair<IAqlExpression, List<VariableSubstitution>> p1 = t.getExpr().accept(this, newSubs);
            QuantifiedPair t2 = new QuantifiedPair(new VariableExpr(newVar), (Expression) p1.first);
            newPairs.add(t2);
        }
        Pair<IAqlExpression, List<VariableSubstitution>> p2 = qe.getSatisfiesExpr().accept(this, newSubs);
        QuantifiedExpression qe2 = new QuantifiedExpression(qe.getQuantifier(), newPairs, (Expression) p2.first);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(qe2, newSubs);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitVariableExpr(VariableExpr v,
            List<VariableSubstitution> arg) throws AsterixException {
        VariableSubstitution vs = findVarSubst(arg, v.getVar());
        VarIdentifier var;
        if (vs != null) {
            // it is a variable subst from the list
            var = vs.getNewVar();
        } else {
            // it is a var. from the context
            var = context.getRewrittenVar(v.getVar().getId());
            if (var == null) {
                var = v.getVar();
            }
        }
        VariableExpr ve = new VariableExpr(var);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(ve, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitWhereClause(WhereClause wc,
            List<VariableSubstitution> arg) throws AsterixException {
        Pair<IAqlExpression, List<VariableSubstitution>> p1 = wc.getWhereExpr().accept(this, arg);
        WhereClause newW = new WhereClause((Expression) p1.first);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newW, p1.second);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitCallExpr(CallExpr pf, List<VariableSubstitution> arg)
            throws AsterixException {
        List<Expression> exprList = visitAndCloneExprList(pf.getExprList(), arg);
        CallExpr f = new CallExpr(pf.getFunctionSignature(), exprList);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(f, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitFunctionDecl(FunctionDecl fd,
            List<VariableSubstitution> arg) throws AsterixException {
        List<VarIdentifier> newList = new ArrayList<VarIdentifier>(fd.getParamList().size());
        for (VarIdentifier vi : fd.getParamList()) {
            VariableSubstitution vs = findVarSubst(arg, vi);
            if (vs == null) {
                throw new AsterixException("Parameter " + vi + " does not appear in the substitution list.");
            }
            newList.add(vs.getNewVar());
        }

        Pair<IAqlExpression, List<VariableSubstitution>> p1 = fd.getFuncBody().accept(this, arg);
        FunctionDecl newF = new FunctionDecl(fd.getSignature(), newList, (Expression) p1.first);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newF, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitIfExpr(IfExpr ifexpr, List<VariableSubstitution> arg)
            throws AsterixException {
        Pair<IAqlExpression, List<VariableSubstitution>> p1 = ifexpr.getCondExpr().accept(this, arg);
        Pair<IAqlExpression, List<VariableSubstitution>> p2 = ifexpr.getThenExpr().accept(this, arg);
        Pair<IAqlExpression, List<VariableSubstitution>> p3 = ifexpr.getElseExpr().accept(this, arg);
        IfExpr i = new IfExpr((Expression) p1.first, (Expression) p2.first, (Expression) p3.first);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(i, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitIndexAccessor(IndexAccessor ia,
            List<VariableSubstitution> arg) throws AsterixException {
        Pair<IAqlExpression, List<VariableSubstitution>> p1 = ia.getExpr().accept(this, arg);
        Expression indexExpr = null;
        if (!ia.isAny()) {
            Pair<IAqlExpression, List<VariableSubstitution>> p2 = ia.getIndexExpr().accept(this, arg);
            indexExpr = (Expression) p2.first;
        }
        IndexAccessor i = new IndexAccessor((Expression) p1.first, indexExpr);
        i.setAny(ia.isAny());
        return new Pair<IAqlExpression, List<VariableSubstitution>>(i, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitLimitClause(LimitClause lc,
            List<VariableSubstitution> arg) throws AsterixException {
        Pair<IAqlExpression, List<VariableSubstitution>> p1 = lc.getLimitExpr().accept(this, arg);
        Pair<IAqlExpression, List<VariableSubstitution>> p2 = lc.getOffset().accept(this, arg);
        LimitClause c = new LimitClause((Expression) p1.first, (Expression) p2.first);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(c, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitListConstructor(ListConstructor lc,
            List<VariableSubstitution> arg) throws AsterixException {
        List<Expression> oldExprList = lc.getExprList();
        List<Expression> exprs = visitAndCloneExprList(oldExprList, arg);
        ListConstructor c = new ListConstructor(lc.getType(), exprs);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(c, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitLiteralExpr(LiteralExpr l,
            List<VariableSubstitution> arg) throws AsterixException {
        // LiteralExpr e = new LiteralExpr(l.getValue());
        return new Pair<IAqlExpression, List<VariableSubstitution>>(l, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitOperatorExpr(OperatorExpr op,
            List<VariableSubstitution> arg) throws AsterixException {
        ArrayList<Expression> oldExprList = op.getExprList();
        ArrayList<Expression> exprs = new ArrayList<Expression>(oldExprList.size());
        for (Expression e : oldExprList) {
            Pair<IAqlExpression, List<VariableSubstitution>> p1 = e.accept(this, arg);
            exprs.add((Expression) p1.first);
        }
        OperatorExpr oe = new OperatorExpr(exprs, op.getExprBroadcastIdx(), op.getOpList());
        oe.setCurrentop(op.isCurrentop());
        return new Pair<IAqlExpression, List<VariableSubstitution>>(oe, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitOrderbyClause(OrderbyClause oc,
            List<VariableSubstitution> arg) throws AsterixException {
        List<Expression> exprList = visitAndCloneExprList(oc.getOrderbyList(), arg);
        OrderbyClause oc2 = new OrderbyClause(exprList, oc.getModifierList());
        oc2.setNumFrames(oc.getNumFrames());
        oc2.setNumTuples(oc.getNumTuples());
        oc2.setRangeMap(oc.getRangeMap());
        return new Pair<IAqlExpression, List<VariableSubstitution>>(oc2, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitQuery(Query q, List<VariableSubstitution> arg)
            throws AsterixException {
        Query newQ = new Query();
        Pair<IAqlExpression, List<VariableSubstitution>> p1 = q.getBody().accept(this, arg);
        newQ.setBody((Expression) p1.first);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newQ, p1.second);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitRecordConstructor(RecordConstructor rc,
            List<VariableSubstitution> arg) throws AsterixException {
        List<FieldBinding> oldFbs = rc.getFbList();
        ArrayList<FieldBinding> newFbs = new ArrayList<FieldBinding>(oldFbs.size());
        for (FieldBinding fb : oldFbs) {
            Pair<IAqlExpression, List<VariableSubstitution>> p1 = fb.getLeftExpr().accept(this, arg);
            Pair<IAqlExpression, List<VariableSubstitution>> p2 = fb.getRightExpr().accept(this, arg);
            FieldBinding fb2 = new FieldBinding((Expression) p1.first, (Expression) p2.first);
            newFbs.add(fb2);
        }
        RecordConstructor newRc = new RecordConstructor(newFbs);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newRc, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitUnaryExpr(UnaryExpr u, List<VariableSubstitution> arg)
            throws AsterixException {
        Pair<IAqlExpression, List<VariableSubstitution>> p1 = u.getExpr().accept(this, arg);
        UnaryExpr newU = new UnaryExpr(u.getSign(), (Expression) p1.first);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newU, arg);
    }

    private List<Expression> visitAndCloneExprList(List<Expression> oldExprList, List<VariableSubstitution> arg)
            throws AsterixException {
        List<Expression> exprs = new ArrayList<Expression>(oldExprList.size());
        for (Expression e : oldExprList) {
            Pair<IAqlExpression, List<VariableSubstitution>> p1 = e.accept(this, arg);
            exprs.add((Expression) p1.first);
        }
        return exprs;
    }

    private static VariableSubstitution findVarSubst(List<VariableSubstitution> varSubstList, VarIdentifier v) {
        VariableSubstitution res = null;
        for (VariableSubstitution s : varSubstList) {
            if (s.getOldVar().getValue().equals(v.getValue())) {
                res = s;
                break;
            }
        }
        return res;
    }

    private static List<VariableSubstitution> eliminateSubstFromList(VarIdentifier vi, List<VariableSubstitution> arg) {
        List<VariableSubstitution> newArg = new LinkedList<VariableSubstitution>();
        for (VariableSubstitution vs1 : arg) {
            if (!vs1.getOldVar().getValue().equals(vi.getValue())) {
                newArg.add(vs1);
            }
        }
        return newArg;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitTypeDecl(TypeDecl td, List<VariableSubstitution> arg)
            throws AsterixException {
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitRecordTypeDefiniton(RecordTypeDefinition tre,
            List<VariableSubstitution> arg) throws AsterixException {
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitTypeReferenceExpression(TypeReferenceExpression tre,
            List<VariableSubstitution> arg) throws AsterixException {
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitNodegroupDecl(NodegroupDecl ngd,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitLoadStatement(LoadStatement stmtLoad,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitDropStatement(DropStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitDisconnectFeedStatement(DisconnectFeedStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    private List<GbyVariableExpressionPair> substInVarExprPair(List<GbyVariableExpressionPair> gbyVeList,
            List<VariableSubstitution> arg, List<VariableSubstitution> newSubs) throws AsterixException {
        List<GbyVariableExpressionPair> veList = new LinkedList<GbyVariableExpressionPair>();
        for (GbyVariableExpressionPair vep : gbyVeList) {
            VariableExpr oldGbyVar = vep.getVar();
            VariableExpr newGbyVar = null;
            if (oldGbyVar != null) {
                VarIdentifier newVar = context.mapOldVarIdentifier(oldGbyVar.getVar());
                newSubs = eliminateSubstFromList(newVar, newSubs);
                newGbyVar = new VariableExpr(newVar);
            }
            Pair<IAqlExpression, List<VariableSubstitution>> p1 = vep.getExpr().accept(this, newSubs);
            GbyVariableExpressionPair ve2 = new GbyVariableExpressionPair(newGbyVar, (Expression) p1.first);
            veList.add(ve2);
        }
        return veList;

    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitCreateIndexStatement(CreateIndexStatement cis,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitUnionExpr(UnionExpr u, List<VariableSubstitution> arg)
            throws AsterixException {
        List<Expression> exprList = visitAndCloneExprList(u.getExprs(), arg);
        UnionExpr newU = new UnionExpr(exprList);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(newU, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitDistinctClause(DistinctClause dc,
            List<VariableSubstitution> arg) throws AsterixException {
        List<Expression> exprList = visitAndCloneExprList(dc.getDistinctByExpr(), arg);
        DistinctClause dc2 = new DistinctClause(exprList);
        return new Pair<IAqlExpression, List<VariableSubstitution>>(dc2, arg);
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitOrderedListTypeDefiniton(
            OrderedListTypeDefinition olte, List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitUnorderedListTypeDefiniton(
            UnorderedListTypeDefinition ulte, List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitInsertStatement(InsertStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitDeleteStatement(DeleteStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitUpdateStatement(UpdateStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitUpdateClause(UpdateClause del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitDataverseDecl(DataverseDecl dv,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitSetStatement(SetStatement ss,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitWriteStatement(WriteStatement ws,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitDatasetDecl(DatasetDecl dd,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitCreateDataverseStatement(CreateDataverseStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitIndexDropStatement(IndexDropStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitNodeGroupDropStatement(NodeGroupDropStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitDataverseDropStatement(DataverseDropStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitTypeDropStatement(TypeDropStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visit(CreateFunctionStatement cfs,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitFunctionDropStatement(FunctionDropStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitCreatePrimaryFeedStatement(
            CreatePrimaryFeedStatement del, List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitCreateSecondaryFeedStatement(
            CreateSecondaryFeedStatement del, List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitConnectFeedStatement(ConnectFeedStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitDropFeedStatement(FeedDropStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitCompactStatement(CompactStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitCreateFeedPolicyStatement(
            CreateFeedPolicyStatement cfps, List<VariableSubstitution> arg) throws AsterixException {
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitDropFeedPolicyStatement(FeedPolicyDropStatement dfs,
            List<VariableSubstitution> arg) throws AsterixException {
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitCreateChannelStatement(CreateChannelStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitDropChannelStatement(ChannelDropStatement del,
            List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitChannelSubscribeStatement(
            ChannelSubscribeStatement del, List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IAqlExpression, List<VariableSubstitution>> visitChannelUnsubscribeStatement(
            ChannelUnsubscribeStatement del, List<VariableSubstitution> arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

}