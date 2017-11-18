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
package org.apache.asterix.lang.common.visitor;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
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
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition.RecordKind;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.DisconnectFeedStatement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.InternalDetailsDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.common.statement.TypeDecl;
import org.apache.asterix.lang.common.statement.WriteStatement;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.visitor.base.AbstractQueryExpressionVisitor;

public abstract class QueryPrintVisitor extends AbstractQueryExpressionVisitor<Void, Integer> {
    protected final PrintWriter out;

    protected QueryPrintVisitor(PrintWriter out) {
        this.out = out;
    }

    protected String skip(int step) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < step; i++) {
            sb.append("  ");
        }
        return sb.toString();
    }

    @Override
    public Void visit(Query q, Integer step) throws CompilationException {
        if (q.getBody() != null) {
            out.println("Query:");
            q.getBody().accept(this, step);
        } else {
            out.println("No query.");
        }
        return null;
    }

    @Override
    public Void visit(LiteralExpr l, Integer step) {
        Literal lc = l.getValue();
        if (lc.getLiteralType().equals(Literal.Type.TRUE) || lc.getLiteralType().equals(Literal.Type.FALSE)
                || lc.getLiteralType().equals(Literal.Type.NULL) || lc.getLiteralType().equals(Literal.Type.MISSING)) {
            out.println(skip(step) + "LiteralExpr [" + l.getValue().getLiteralType() + "]");
        } else {
            out.println(skip(step) + "LiteralExpr [" + l.getValue().getLiteralType() + "] ["
                    + l.getValue().getStringValue() + "]");
        }
        return null;
    }

    @Override
    public Void visit(VariableExpr v, Integer step) {
        out.println(skip(step) + "Variable [ Name=" + v.getVar().getValue() + " ]");
        return null;
    }

    @Override
    public Void visit(ListConstructor lc, Integer step) throws CompilationException {
        boolean ordered = false;
        if (lc.getType().equals(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR)) {
            ordered = true;
        }

        out.println(skip(step) + (ordered ? "OrderedListConstructor " : "UnorderedListConstructor ") + "[");
        for (Expression e : lc.getExprList()) {
            e.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(RecordConstructor rc, Integer step) throws CompilationException {
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
        return null;
    }

    @Override
    public Void visit(CallExpr pf, Integer step) throws CompilationException {
        out.println(skip(step) + "FunctionCall " + pf.getFunctionSignature().toString() + "[");
        for (Expression expr : pf.getExprList()) {
            expr.accept(this, step + 1);
        }
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(OperatorExpr ifbo, Integer step) throws CompilationException {
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
        return null;
    }

    @Override
    public Void visit(IfExpr ifexpr, Integer step) throws CompilationException {
        out.println(skip(step) + "IfExpr [");
        out.println(skip(step + 1) + "Condition:");
        ifexpr.getCondExpr().accept(this, step + 2);
        out.println(skip(step + 1) + "Then:");
        ifexpr.getThenExpr().accept(this, step + 2);
        out.println(skip(step + 1) + "Else:");
        ifexpr.getElseExpr().accept(this, step + 2);
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(QuantifiedExpression qe, Integer step) throws CompilationException {
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
        return null;
    }

    @Override
    public Void visit(LetClause lc, Integer step) throws CompilationException {
        out.print(skip(step) + "Let ");
        lc.getVarExpr().accept(this, 0);
        out.println(skip(step + 1) + ":=");
        lc.getBindingExpr().accept(this, step + 1);
        return null;
    }

    @Override
    public Void visit(WhereClause wc, Integer step) throws CompilationException {
        out.println(skip(step) + "Where");
        wc.getWhereExpr().accept(this, step + 1);
        return null;
    }

    @Override
    public Void visit(OrderbyClause oc, Integer step) throws CompilationException {
        out.println(skip(step) + "Orderby");
        List<OrderModifier> mlist = oc.getModifierList();
        List<Expression> list = oc.getOrderbyList();
        for (int i = 0; i < list.size(); i++) {
            list.get(i).accept(this, step + 1);
            out.println(skip(step + 1) + mlist.get(i).toString());
        }
        out.println();
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Integer step) throws CompilationException {
        out.println(skip(step) + "Groupby");
        for (GbyVariableExpressionPair pair : gc.getGbyPairList()) {
            if (pair.getVar() != null) {
                pair.getVar().accept(this, step + 1);
                out.println(skip(step + 1) + ":=");
            }
            pair.getExpr().accept(this, step + 1);
        }
        if (gc.hasDecorList()) {
            out.println(skip(step + 1) + "Decor");
            for (GbyVariableExpressionPair pair : gc.getDecorPairList()) {
                if (pair.getVar() != null) {
                    pair.getVar().accept(this, step + 1);
                    out.println(skip(step + 1) + ":=");
                }
                pair.getExpr().accept(this, step + 1);
            }
        }
        if (gc.hasWithMap()) {
            out.println(skip(step + 1) + "With");
            for (Entry<Expression, VariableExpr> entry : gc.getWithVarMap().entrySet()) {
                Expression key = entry.getKey();
                VariableExpr value = entry.getValue();
                key.accept(this, step + 1);
                if (!key.equals(value)) {
                    out.println(skip(step + 1) + "AS");
                    value.accept(this, step + 1);
                }
            }
        }
        out.println();
        return null;
    }

    @Override
    public Void visit(LimitClause lc, Integer step) throws CompilationException {
        out.println(skip(step) + "Limit");
        lc.getLimitExpr().accept(this, step + 1);
        if (lc.getOffset() != null) {
            out.println(skip(step + 1) + "Offset");
            lc.getOffset().accept(this, step + 2);
        }
        return null;
    }

    @Override
    public Void visit(FunctionDecl fd, Integer step) throws CompilationException {
        out.println(skip(step) + "FunctionDecl " + fd.getSignature().getName() + "(" + fd.getParamList().toString()
                + ") {");
        fd.getFuncBody().accept(this, step + 1);
        out.println(skip(step) + "}");
        out.println();
        return null;
    }

    @Override
    public Void visit(UnaryExpr u, Integer step) throws CompilationException {
        if (u.getExprType() != null) {
            out.print(skip(step) + u.getExprType() + " ");
            u.getExpr().accept(this, 0);
        } else {
            u.getExpr().accept(this, step);
        }
        return null;
    }

    @Override
    public Void visit(FieldAccessor fa, Integer step) throws CompilationException {
        out.println(skip(step) + "FieldAccessor [");
        fa.getExpr().accept(this, step + 1);
        out.println(skip(step + 1) + "Field=" + fa.getIdent().getValue());
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(IndexAccessor fa, Integer step) throws CompilationException {
        out.println(skip(step) + "IndexAccessor [");
        fa.getExpr().accept(this, step + 1);
        out.print(skip(step + 1) + "Index: ");
        if (fa.isAny()) {
            out.println("ANY");
        } else {
            fa.getIndexExpr().accept(this, step + 1);
        }
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(TypeDecl t, Integer step) throws CompilationException {
        out.println(skip(step) + "TypeDecl " + t.getIdent() + " [");
        t.getTypeDef().accept(this, step + 1);
        out.println(skip(step) + "]");
        return null;
    }

    @Override
    public Void visit(TypeReferenceExpression t, Integer arg) throws CompilationException {
        if (t.getIdent().first != null && t.getIdent().first.getValue() != null) {
            out.print(t.getIdent().first.getValue());
            out.print('.');
        }
        out.print(t.getIdent().second.getValue());
        return null;
    }

    @Override
    public Void visit(RecordTypeDefinition r, Integer step) throws CompilationException {
        if (r.getRecordKind() == RecordKind.CLOSED) {
            out.print(skip(step) + "closed ");
        } else {
            out.print(skip(step) + "open ");
        }
        out.println("RecordType {");
        Iterator<String> nameIter = r.getFieldNames().iterator();
        Iterator<TypeExpression> typeIter = r.getFieldTypes().iterator();
        Iterator<Boolean> isOptionalIter = r.getOptionableFields().iterator();
        boolean first = true;
        while (nameIter.hasNext()) {
            if (first) {
                first = false;
            } else {
                out.println(",");
            }
            String name = nameIter.next();
            TypeExpression texp = typeIter.next();
            Boolean isNullable = isOptionalIter.next();
            out.print(skip(step + 1) + name + " : ");
            texp.accept(this, step + 2);
            if (isNullable) {
                out.print("?");
            }
        }
        out.println();
        out.println(skip(step) + "}");
        return null;
    }

    @Override
    public Void visit(OrderedListTypeDefinition x, Integer step) throws CompilationException {
        out.print("OrderedList [");
        x.getItemTypeExpression().accept(this, step + 2);
        out.println("]");
        return null;
    }

    @Override
    public Void visit(UnorderedListTypeDefinition x, Integer step) throws CompilationException {
        out.print("UnorderedList <");
        x.getItemTypeExpression().accept(this, step + 2);
        out.println(">");
        return null;
    }

    @Override
    public Void visit(DatasetDecl dd, Integer step) throws CompilationException {
        if (dd.getDatasetType() == DatasetType.INTERNAL) {
            String line = skip(step) + "DatasetDecl " + dd.getName() + "(" + dd.getItemTypeName() + ")"
                    + " partitioned by " + ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getPartitioningExprs();
            if (((InternalDetailsDecl) dd.getDatasetDetailsDecl()).isAutogenerated()) {
                line += " [autogenerated]";
            }
            out.println(line);
        } else if (dd.getDatasetType() == DatasetType.EXTERNAL) {
            out.println(skip(step) + "DatasetDecl " + dd.getName() + "(" + dd.getItemTypeName() + ")"
                    + "is an external dataset");
        }
        return null;
    }

    @Override
    public Void visit(DataverseDecl dv, Integer step) throws CompilationException {
        out.println(skip(step) + "DataverseUse " + dv.getDataverseName());
        return null;
    }

    @Override
    public Void visit(WriteStatement ws, Integer step) throws CompilationException {
        out.print(skip(step) + "WriteOutputTo " + ws.getNcName() + ":" + ws.getFileName());
        if (ws.getWriterClassName() != null) {
            out.print(" using " + ws.getWriterClassName());
        }
        out.println();
        return null;
    }

    @Override
    public Void visit(SetStatement ss, Integer step) throws CompilationException {
        out.println(skip(step) + "Set " + ss.getPropName() + "=" + ss.getPropValue());
        return null;
    }

    @Override
    public Void visit(DisconnectFeedStatement ss, Integer step) throws CompilationException {
        out.println(skip(step) + skip(step) + ss.getFeedName() + skip(step) + ss.getDatasetName());
        return null;
    }

}
