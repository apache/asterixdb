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
package edu.uci.ics.asterix.aql.expression.visitor;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Literal;
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
import edu.uci.ics.asterix.aql.expression.InternalDetailsDecl;
import edu.uci.ics.asterix.aql.expression.LetClause;
import edu.uci.ics.asterix.aql.expression.LimitClause;
import edu.uci.ics.asterix.aql.expression.ListConstructor;
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
import edu.uci.ics.asterix.aql.expression.QuantifiedPair;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.RecordConstructor;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition.RecordKind;
import edu.uci.ics.asterix.aql.expression.SetStatement;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.TypeDropStatement;
import edu.uci.ics.asterix.aql.expression.TypeExpression;
import edu.uci.ics.asterix.aql.expression.TypeReferenceExpression;
import edu.uci.ics.asterix.aql.expression.UnaryExpr;
import edu.uci.ics.asterix.aql.expression.UnionExpr;
import edu.uci.ics.asterix.aql.expression.UnorderedListTypeDefinition;
import edu.uci.ics.asterix.aql.expression.UpdateClause;
import edu.uci.ics.asterix.aql.expression.UpdateStatement;
import edu.uci.ics.asterix.aql.expression.VariableExpr;
import edu.uci.ics.asterix.aql.expression.WhereClause;
import edu.uci.ics.asterix.aql.expression.WriteStatement;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

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
        out.println(skip(step + 1) + "Field=" + ((FieldAccessor) fa).getIdent().getValue());
        out.println(skip(step) + "]");

    }

    @Override
    public void visit(IndexAccessor fa, Integer step) throws AsterixException {
        out.println(skip(step) + "IndexAccessor [");
        fa.getExpr().accept(this, step + 1);
        out.print(skip(step + 1) + "Index: ");
        out.println((((IndexAccessor) fa).isAny() ? "ANY" : ((IndexAccessor) fa).getIndex()));

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
            out.println(skip(step) + "DatasetDecl" + dd.getName() + "(" + dd.getItemTypeName() + ")"
                    + " partitioned by " + ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getPartitioningExprs());
        } else if (dd.getDatasetType() == DatasetType.EXTERNAL) {
            out.println(skip(step) + "DatasetDecl" + dd.getName() + "(" + dd.getItemTypeName() + ")"
                    + "is an external dataset");
        } else if (dd.getDatasetType() == DatasetType.FEED) {
            out.println(skip(step) + "DatasetDecl" + dd.getName() + "(" + dd.getItemTypeName() + ")"
                    + "is an feed dataset");
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
    public void visit(LoadFromFileStatement stmtLoad, Integer arg) throws AsterixException {
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
    public void visit(ControlFeedStatement ss, Integer step) throws AsterixException {
        out.println(skip(step) + ss.getOperationType() + skip(step) + ss.getDatasetName());
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
    public void visit(BeginFeedStatement stmtDel, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub

    }

    @Override
    public void visit(CompactStatement fds, Integer arg) throws AsterixException {
        // TODO Auto-generated method stub
        
    }

}
