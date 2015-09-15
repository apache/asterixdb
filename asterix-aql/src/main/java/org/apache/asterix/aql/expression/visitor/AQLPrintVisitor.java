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
package org.apache.asterix.aql.expression.visitor;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.aql.base.Clause;
import org.apache.asterix.aql.base.Expression;
import org.apache.asterix.aql.base.Literal;
import org.apache.asterix.aql.expression.CallExpr;
import org.apache.asterix.aql.expression.CompactStatement;
import org.apache.asterix.aql.expression.ConnectFeedStatement;
import org.apache.asterix.aql.expression.CreateDataverseStatement;
import org.apache.asterix.aql.expression.CreateFeedPolicyStatement;
import org.apache.asterix.aql.expression.CreateFeedStatement;
import org.apache.asterix.aql.expression.CreateFunctionStatement;
import org.apache.asterix.aql.expression.CreateIndexStatement;
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
import org.apache.asterix.aql.expression.InternalDetailsDecl;
import org.apache.asterix.aql.expression.LetClause;
import org.apache.asterix.aql.expression.LimitClause;
import org.apache.asterix.aql.expression.ListConstructor;
import org.apache.asterix.aql.expression.LiteralExpr;
import org.apache.asterix.aql.expression.LoadStatement;
import org.apache.asterix.aql.expression.NodeGroupDropStatement;
import org.apache.asterix.aql.expression.NodegroupDecl;
import org.apache.asterix.aql.expression.OperatorExpr;
import org.apache.asterix.aql.expression.OperatorType;
import org.apache.asterix.aql.expression.OrderbyClause;
import org.apache.asterix.aql.expression.OrderbyClause.OrderModifier;
import org.apache.asterix.aql.expression.OrderedListTypeDefinition;
import org.apache.asterix.aql.expression.QuantifiedExpression;
import org.apache.asterix.aql.expression.QuantifiedPair;
import org.apache.asterix.aql.expression.Query;
import org.apache.asterix.aql.expression.RecordConstructor;
import org.apache.asterix.aql.expression.RecordTypeDefinition;
import org.apache.asterix.aql.expression.RecordTypeDefinition.RecordKind;
import org.apache.asterix.aql.expression.RunStatement;
import org.apache.asterix.aql.expression.SetStatement;
import org.apache.asterix.aql.expression.TypeDecl;
import org.apache.asterix.aql.expression.TypeDropStatement;
import org.apache.asterix.aql.expression.TypeExpression;
import org.apache.asterix.aql.expression.TypeReferenceExpression;
import org.apache.asterix.aql.expression.UnaryExpr;
import org.apache.asterix.aql.expression.UnionExpr;
import org.apache.asterix.aql.expression.UnorderedListTypeDefinition;
import org.apache.asterix.aql.expression.UpdateClause;
import org.apache.asterix.aql.expression.UpdateStatement;
import org.apache.asterix.aql.expression.VariableExpr;
import org.apache.asterix.aql.expression.WhereClause;
import org.apache.asterix.aql.expression.WriteStatement;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.AsterixException;

public class AQLPrintVisitor implements IAqlVisitorWithVoidReturn<Integer> {
    // private int level =0;
    private final PrintWriter out;

    public AQLPrintVisitor() {
        out = new PrintWriter(System.out);
    }

    public AQLPrintVisitor(PrintWriter out) {
        this.out = out;
    }

    private String skip(int step) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < step; i++)
            sb.append("  ");
        return sb.toString();
    }

    @Override
    public void visit(Query q, Integer step) throws AsterixException {
        if (q.getBody() != null) {
            out.println("Query:");
            q.getBody().accept(this, step);
        } else {
            out.println("No query.");
        }
    }

    @Override
    public void visit(LiteralExpr l, Integer step) {
        Literal lc = l.getValue();
        if (lc.getLiteralType().equals(Literal.Type.TRUE) || lc.getLiteralType().equals(Literal.Type.FALSE)
                || lc.getLiteralType().equals(Literal.Type.NULL)) {
            out.println(skip(step) + "LiteralExpr [" + l.getValue().getLiteralType() + "]");
        } else {
            out.println(skip(step) + "LiteralExpr [" + l.getValue().getLiteralType() + "] ["
                    + l.getValue().getStringValue() + "] ");
        }
    }

    @Override
    public void visit(VariableExpr v, Integer step) {
        out.println(skip(step) + "Variable [ Name=" + v.getVar().getValue() + " Id=" + v.getVar().getId() + " ]");
    }

    @Override
    public void visit(ListConstructor lc, Integer step) throws AsterixException {
        boolean ordered = false;
        if (lc.getType().equals(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR)) {
            ordered = true;
        }

        out.println(skip(step) + (ordered == true ? "OrderedListConstructor " : "UnorderedListConstructor ") + "[");
        for (Expression e : lc.getExprList()) {
            e.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
    }

    @Override
    public void visit(RecordConstructor rc, Integer step) throws AsterixException {
        out.println(skip(step) + "RecordConstructor [");
        // fbList accept visitor
        for (FieldBinding fb : rc.getFbList()) {
            out.println(skip(step + 1) + "(");
            fb.getLeftExpr().accept(this, step + 2);
            out.println(skip(step + 2) + ":");
            fb.getRightExpr().accept(this, step + 2);
            out.println(skip(step + 1) + ")");
        }
        out.println(skip(step) + "]");
    }

    @Override
    public void visit(CallExpr pf, Integer step) throws AsterixException {
        out.println(skip(step) + "FunctionCall " + pf.getFunctionSignature().toString() + "[");
        for (Expression expr : pf.getExprList()) {
            expr.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
    }

    @Override
    public void visit(OperatorExpr ifbo, Integer step) throws AsterixException {
        List<Expression> exprList = ifbo.getExprList();
        List<OperatorType> opList = ifbo.getOpList();
        if (ifbo.isCurrentop()) {
            out.println(skip(step) + "OperatorExpr [");
            exprList.get(0).accept(this, step + 1);
            for (int i = 1; i < exprList.size(); i++) {
                out.println(skip(step + 1) + opList.get(i - 1));
                exprList.get(i).accept(this, step + 1);
            }
            out.println(skip(step) + "]");
        } else {
            exprList.get(0).accept(this, step);
        }

    }

    @Override
    public void visit(IfExpr ifexpr, Integer step) throws AsterixException {
        out.println(skip(step) + "IfExpr [");
        out.println(skip(step + 1) + "Condition:");
        ifexpr.getCondExpr().accept(this, step + 2);
        out.println(skip(step + 1) + "Then:");
        ifexpr.getThenExpr().accept(this, step + 2);
        out.println(skip(step + 1) + "Else:");
        ifexpr.getElseExpr().accept(this, step + 2);
        out.println(skip(step) + "]");
    }

    @Override
    public void visit(FLWOGRExpression flwor, Integer step) throws AsterixException {
        out.println(skip(step) + "FLWOGR [");
        for (Clause cl : flwor.getClauseList()) {
            cl.accept(this, step + 1);
        }
        out.println(skip(step + 1) + "Return");
        flwor.getReturnExpr().accept(this, step + 2);
        out.println(skip(step) + "]");
    }

    @Override
    public void visit(QuantifiedExpression qe, Integer step) throws AsterixException {
        out.println(skip(step) + "QuantifiedExpression " + qe.getQuantifier() + " [");
        // quantifiedList accept visitor
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            out.print(skip(step + 1) + "[");
            pair.getVarExpr().accept(this, 0);
            out.println(skip(step + 1) + "In");
            pair.getExpr().accept(this, step + 2);
            out.println(skip(step + 1) + "]");
        }
        out.println(skip(step + 1) + "Satifies [");
        qe.getSatisfiesExpr().accept(this, step + 2);
        out.println(skip(step + 1) + "]");// for satifies
        out.println(skip(step) + "]");// for quantifiedExpr
    }

    @Override
    public void visit(ForClause fc, Integer step) throws AsterixException {
        out.print(skip(step) + "For ");
        fc.getVarExpr().accept(this, 0);
        out.println(skip(step + 1) + "In ");
        fc.getInExpr().accept(this, step + 1);
    }

    @Override
    public void visit(LetClause lc, Integer step) throws AsterixException {
        out.print(skip(step) + "Let ");
        lc.getVarExpr().accept(this, 0);
        out.println(skip(step + 1) + ":= ");
        lc.getBindingExpr().accept(this, step + 1);
    }

    @Override
    public void visit(WhereClause wc, Integer step) throws AsterixException {
        out.println(skip(step) + "Where ");
        wc.getWhereExpr().accept(this, step + 1);
    }

    @Override
    public void visit(OrderbyClause oc, Integer step) throws AsterixException {
        out.println(skip(step) + "Orderby");
        List<OrderModifier> mlist = oc.getModifierList();
        List<Expression> list = oc.getOrderbyList();
        for (int i = 0; i < list.size(); i++) {
            list.get(i).accept(this, step + 1);
            out.println(skip(step + 1) + mlist.get(i).toString());
        }
        out.println(skip(step));
    }

    @Override
    public void visit(GroupbyClause gc, Integer step) throws AsterixException {
        out.println(skip(step) + "Groupby");
        for (GbyVariableExpressionPair pair : gc.getGbyPairList()) {

            if (pair.getVar() != null) {
                pair.getVar().accept(this, step + 1);
                out.println(skip(step + 1) + ":=");
            }

            pair.getExpr().accept(this, step + 1);
        }
        if (!gc.getDecorPairList().isEmpty()) {
            out.println(skip(step + 1) + "Decor");
            for (GbyVariableExpressionPair pair : gc.getDecorPairList()) {
                if (pair.getVar() != null) {
                    pair.getVar().accept(this, step + 1);
                    out.println(skip(step + 1) + ":=");
                }
                pair.getExpr().accept(this, step + 1);
            }
        }
        out.println(skip(step + 1) + "With");
        for (VariableExpr exp : gc.getWithVarList()) {
            exp.accept(this, step + 1);
        }
        out.println(skip(step));
    }

    @Override
    public void visit(LimitClause lc, Integer step) throws AsterixException {
        out.println(skip(step) + "Limit");
        lc.getLimitExpr().accept(this, step + 1);
        if (lc.getOffset() != null) {
            out.println(skip(step + 1) + "Offset");
            lc.getOffset().accept(this, step + 2);
        }
    }

    @Override
    public void visit(FunctionDecl fd, Integer step) throws AsterixException {
        out.println(skip(step) + "FunctionDecl " + fd.getSignature().getName() + "(" + fd.getParamList().toString()
                + ") {");
        fd.getFuncBody().accept(this, step + 1);
        out.println(skip(step) + "}");
        out.println();
    }

    @Override
    public void visit(UnaryExpr u, Integer step) throws AsterixException {
        if (u.getSign() != null) {
            out.print(skip(step) + u.getSign() + " ");
            u.getExpr().accept(this, 0);
        } else
            u.getExpr().accept(this, step);
    }

    @Override
    public void visit(FieldAccessor fa, Integer step) throws AsterixException {
        out.println(skip(step) + "FieldAccessor [");
        fa.getExpr().accept(this, step + 1);
        out.println(skip(step + 1) + "Field=" + fa.getIdent().getValue());
        out.println(skip(step) + "]");

    }

    @Override
    public void visit(IndexAccessor fa, Integer step) throws AsterixException {
        out.println(skip(step) + "IndexAccessor [");
        fa.getExpr().accept(this, step + 1);
        out.print(skip(step + 1) + "Index: ");
        if (fa.isAny()) {
            out.println("ANY");
        } else {
            fa.getIndexExpr().accept(this, step + 1);
        }
        out.println(skip(step) + "]");
    }

    @Override
    public void visit(TypeDecl t, Integer step) throws AsterixException {
        out.println(skip(step) + "TypeDecl " + t.getIdent() + " [");
        t.getTypeDef().accept(this, step + 1);
        out.println(skip(step) + "]");
    }

    @Override
    public void visit(TypeReferenceExpression t, Integer arg) throws AsterixException {
        out.print(t.getIdent());
    }

    @Override
    public void visit(RecordTypeDefinition r, Integer step) throws AsterixException {
        if (r.getRecordKind() == RecordKind.CLOSED) {
            out.print(skip(step) + "closed ");
        } else {
            out.print(skip(step) + "open ");
        }
        out.println("RecordType {");
        Iterator<String> nameIter = r.getFieldNames().iterator();
        Iterator<TypeExpression> typeIter = r.getFieldTypes().iterator();
        Iterator<Boolean> isnullableIter = r.getNullableFields().iterator();
        boolean first = true;
        while (nameIter.hasNext()) {
            if (first) {
                first = false;
            } else {
                out.println(",");
            }
            String name = nameIter.next();
            TypeExpression texp = typeIter.next();
            Boolean isNullable = isnullableIter.next();
            out.print(skip(step + 1) + name + " : ");
            texp.accept(this, step + 2);
            if (isNullable) {
                out.print("?");
            }
        }
        out.println();
        out.println(skip(step) + "}");
    }

    @Override
    public void visit(OrderedListTypeDefinition x, Integer step) throws AsterixException {
        out.print("OrderedList [");
        x.getItemTypeExpression().accept(this, step + 2);
        out.println("]");
    }

    @Override
    public void visit(UnorderedListTypeDefinition x, Integer step) throws AsterixException {
        out.print("UnorderedList <");
        x.getItemTypeExpression().accept(this, step + 2);
        out.println(">");
    }

    @Override
    public void visit(DatasetDecl dd, Integer step) throws AsterixException {
        if (dd.getDatasetType() == DatasetType.INTERNAL) {
            String line = skip(step) + "DatasetDecl" + dd.getName() + "(" + dd.getItemTypeName() + ")"
                    + " partitioned by " + ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getPartitioningExprs();
            if (((InternalDetailsDecl) dd.getDatasetDetailsDecl()).isAutogenerated()) {
                line += " [autogenerated]";
            }
            out.println(line);
        } else if (dd.getDatasetType() == DatasetType.EXTERNAL) {
            out.println(skip(step) + "DatasetDecl" + dd.getName() + "(" + dd.getItemTypeName() + ")"
                    + "is an external dataset");
        }
    }

    @Override
    public void visit(DataverseDecl dv, Integer step) throws AsterixException {
        out.println(skip(step) + "DataverseUse " + dv.getDataverseName());
    }

    @Override
    public void visit(NodegroupDecl ngd, Integer step) throws AsterixException {
        out.println(skip(step) + "Nodegroup " + ngd.getNodeControllerNames());
    }

    @Override
    public void visit(LoadStatement stmtLoad, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(DropStatement stmtDel, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(WriteStatement ws, Integer step) throws AsterixException {
        out.print(skip(step) + "WriteOutputTo " + ws.getNcName() + ":" + ws.getFileName());
        if (ws.getWriterClassName() != null) {
            out.print(" using " + ws.getWriterClassName());
        }
        out.println();
    }

    @Override
    public void visit(SetStatement ss, Integer step) throws AsterixException {
        out.println(skip(step) + "Set " + ss.getPropName() + "=" + ss.getPropValue());
    }

    @Override
    public void visit(CreateIndexStatement cis, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(DisconnectFeedStatement ss, Integer step) throws AsterixException {
        out.println(skip(step) + skip(step) + ss.getFeedName() + skip(step) + ss.getDatasetName());
    }

    @Override
    public void visit(UnionExpr u, Integer step) throws AsterixException {
        out.println(skip(step) + "Union [");
        for (Expression expr : u.getExprs()) {
            expr.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
    }

    @Override
    public void visit(DistinctClause dc, Integer step) throws AsterixException {
        out.print(skip(step) + "Distinct ");
        for (Expression expr : dc.getDistinctByExpr())
            expr.accept(this, step + 1);
    }

    @Override
    public void visit(InsertStatement stmtDel, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(DeleteStatement stmtDel, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(UpdateStatement stmtDel, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(UpdateClause updateClause, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(CreateDataverseStatement stmtDel, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub
    }

    @Override
    public void visit(IndexDropStatement stmtDel, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub
    }

    @Override
    public void visit(NodeGroupDropStatement deleteStatement, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub
    }

    @Override
    public void visit(DataverseDropStatement deleteStatement, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub
    }

    @Override
    public void visit(TypeDropStatement deleteStatement, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub
    }

    @Override
    public void visit(CreateFunctionStatement cfs, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(FunctionDropStatement fds, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(CreateFeedStatement stmtDel, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(ConnectFeedStatement stmtDel, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(FeedDropStatement stmt, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(CompactStatement fds, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }
    
    @Override
    public void visit(CreateFeedPolicyStatement stmt, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub
    }

    @Override
    public void visit(FeedPolicyDropStatement stmt, Integer arg) throws AsterixException {
    }

    @Override
    public void visit(RunStatement stmt, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

}
