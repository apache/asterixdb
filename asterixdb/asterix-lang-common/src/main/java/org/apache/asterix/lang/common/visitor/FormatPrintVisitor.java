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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
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
import org.apache.asterix.lang.common.expression.IndexedTypeExpression;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
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
import org.apache.asterix.lang.common.statement.CompactStatement;
import org.apache.asterix.lang.common.statement.ConnectFeedStatement;
import org.apache.asterix.lang.common.statement.CreateDataverseStatement;
import org.apache.asterix.lang.common.statement.CreateFeedPolicyStatement;
import org.apache.asterix.lang.common.statement.CreateFeedStatement;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.DisconnectFeedStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.ExternalDetailsDecl;
import org.apache.asterix.lang.common.statement.FeedDropStatement;
import org.apache.asterix.lang.common.statement.FeedPolicyDropStatement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.InternalDetailsDecl;
import org.apache.asterix.lang.common.statement.LoadStatement;
import org.apache.asterix.lang.common.statement.NodeGroupDropStatement;
import org.apache.asterix.lang.common.statement.NodegroupDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.common.statement.StartFeedStatement;
import org.apache.asterix.lang.common.statement.StopFeedStatement;
import org.apache.asterix.lang.common.statement.TypeDecl;
import org.apache.asterix.lang.common.statement.TypeDropStatement;
import org.apache.asterix.lang.common.statement.UpdateStatement;
import org.apache.asterix.lang.common.statement.WriteStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.UnaryExprType;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public class FormatPrintVisitor implements ILangVisitor<Void, Integer> {

    protected final static String COMMA = ",";
    protected final static String SEMICOLON = ";";
    private final static String CREATE = "create ";
    private final static String FEED = " feed ";
    private final static String DEFAULT_DATAVERSE_FORMAT = "org.apache.asterix.runtime.formats.NonTaggedDataFormat";
    protected final PrintWriter out;
    protected Set<Character> validIdentifierChars = new HashSet<>();
    protected Set<Character> validIdentifierStartChars = new HashSet<>();
    protected String dataverseSymbol = " dataverse ";
    protected String datasetSymbol = " dataset ";
    protected String assignSymbol = ":=";

    public FormatPrintVisitor(PrintWriter out) {
        this.out = out;
        for (char ch = 'a'; ch <= 'z'; ++ch) {
            validIdentifierChars.add(ch);
            validIdentifierStartChars.add(ch);
        }
        for (char ch = 'A'; ch <= 'Z'; ++ch) {
            validIdentifierChars.add(ch);
            validIdentifierStartChars.add(ch);
        }
        for (char ch = '0'; ch <= '9'; ++ch) {
            validIdentifierChars.add(ch);
        }
        validIdentifierChars.add('_');
        validIdentifierChars.add('$');
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
            q.getBody().accept(this, step);
        }
        if (q.isTopLevel()) {
            out.println(SEMICOLON);
        }
        return null;
    }

    @Override
    public Void visit(LiteralExpr l, Integer step) {
        Literal lc = l.getValue();
        if (lc.getLiteralType().equals(Literal.Type.TRUE) || lc.getLiteralType().equals(Literal.Type.FALSE)
                || lc.getLiteralType().equals(Literal.Type.NULL) || lc.getLiteralType().equals(Literal.Type.MISSING)) {
            out.print(lc.getLiteralType().toString().toLowerCase());
        } else if (lc.getLiteralType().equals(Literal.Type.STRING)) {
            out.print(revertStringToLiteral(lc.getStringValue()));
        } else {
            if (lc.getLiteralType().equals(Literal.Type.FLOAT)) {
                out.printf("%ff", lc.getValue());
            } else if (lc.getLiteralType().equals(Literal.Type.DOUBLE)) {
                DecimalFormat df = new DecimalFormat("#.#");
                df.setMinimumFractionDigits(1);
                df.setMaximumFractionDigits(16);
                out.print(df.format(lc.getValue()));
            } else {
                out.print(lc.getStringValue());
            }
        }
        return null;
    }

    @Override
    public Void visit(VariableExpr v, Integer step) {
        out.print(v.getVar().getValue());
        return null;
    }

    @Override
    public Void visit(ListConstructor lc, Integer step) throws CompilationException {
        boolean ordered = false;
        if (lc.getType().equals(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR)) {
            ordered = true;
        }
        out.print(ordered ? "[" : "{{");
        printDelimitedExpressions(lc.getExprList(), COMMA, step + 2);
        out.print(ordered ? "]" : "}}");
        return null;
    }

    @Override
    public Void visit(RecordConstructor rc, Integer step) throws CompilationException {
        out.print("{");
        // print all field bindings
        int size = rc.getFbList().size();
        int index = 0;
        for (FieldBinding fb : rc.getFbList()) {
            fb.getLeftExpr().accept(this, step + 2);
            out.print(":");
            fb.getRightExpr().accept(this, step + 2);
            if (++index < size) {
                out.print(COMMA);
            }
        }
        out.print("}");
        return null;
    }

    @Override
    public Void visit(CallExpr callExpr, Integer step) throws CompilationException {
        printHints(callExpr.getHints(), step);
        out.print(generateFullName(callExpr.getFunctionSignature().getNamespace(),
                callExpr.getFunctionSignature().getName()) + "(");
        printDelimitedExpressions(callExpr.getExprList(), COMMA, step);
        out.print(")");
        return null;
    }

    @Override
    public Void visit(OperatorExpr operatorExpr, Integer step) throws CompilationException {
        List<Expression> exprList = operatorExpr.getExprList();
        List<OperatorType> opList = operatorExpr.getOpList();
        if (operatorExpr.isCurrentop()) {
            out.print("(");
            exprList.get(0).accept(this, step + 1);
            for (int i = 1; i < exprList.size(); i++) {
                OperatorType opType = opList.get(i - 1);
                if (i == 1) {
                    printHints(operatorExpr.getHints(), step + 1);
                }
                out.print(" " + opType + " ");
                exprList.get(i).accept(this, step + 1);
            }
            out.print(")");
        } else {
            exprList.get(0).accept(this, step);
        }
        return null;
    }

    @Override
    public Void visit(IfExpr ifexpr, Integer step) throws CompilationException {
        out.print("if (");
        ifexpr.getCondExpr().accept(this, step + 2);
        out.println(")");
        out.print(skip(step) + "then ");
        ifexpr.getThenExpr().accept(this, step + 2);
        out.println();
        out.print(skip(step) + "else ");
        ifexpr.getElseExpr().accept(this, step + 2);
        return null;
    }

    @Override
    public Void visit(QuantifiedExpression qe, Integer step) throws CompilationException {
        out.print(qe.getQuantifier().toString().toLowerCase() + " ");
        // quantifiedList accept visitor
        int index = 0;
        int size = qe.getQuantifiedList().size();
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.getVarExpr().accept(this, 0);
            out.print(" in ");
            pair.getExpr().accept(this, step + 2);
            if (++index < size) {
                out.println(COMMA);
            }
        }
        out.print(" satisfies ");
        qe.getSatisfiesExpr().accept(this, step + 2);
        return null;
    }

    @Override
    public Void visit(LetClause lc, Integer step) throws CompilationException {
        out.print(skip(step) + "let ");
        lc.getVarExpr().accept(this, 0);
        out.print(assignSymbol);
        lc.getBindingExpr().accept(this, step + 1);
        out.println();
        return null;
    }

    @Override
    public Void visit(WhereClause wc, Integer step) throws CompilationException {
        out.print(skip(step) + "where ");
        wc.getWhereExpr().accept(this, step + 1);
        out.println();
        return null;
    }

    @Override
    public Void visit(OrderbyClause oc, Integer step) throws CompilationException {
        out.print(skip(step) + "order by ");
        printDelimitedObyExpressions(oc.getOrderbyList(), oc.getModifierList(), step);
        out.println();
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Integer step) throws CompilationException {
        if (gc.hasHashGroupByHint()) {
            out.println(skip(step) + "/* +hash */");
        }
        out.print(skip(step) + "group by ");
        printDelimitedGbyExpressions(gc.getGbyPairList(), step + 2);
        if (gc.hasDecorList()) {
            out.print(" decor ");
            printDelimitedGbyExpressions(gc.getDecorPairList(), step + 2);
        }
        if (gc.hasWithMap()) {
            out.print(" with ");
            Map<Expression, VariableExpr> withVarMap = gc.getWithVarMap();
            int index = 0;
            int size = withVarMap.size();
            for (Entry<Expression, VariableExpr> entry : withVarMap.entrySet()) {
                Expression key = entry.getKey();
                VariableExpr value = entry.getValue();
                key.accept(this, step + 2);
                if (!key.equals(value)) {
                    out.print(" as ");
                    value.accept(this, step + 2);
                }
                if (++index < size) {
                    out.print(COMMA);
                }
            }
        }
        out.println();
        return null;
    }

    @Override
    public Void visit(LimitClause lc, Integer step) throws CompilationException {
        out.print(skip(step) + "limit ");
        lc.getLimitExpr().accept(this, step + 1);
        if (lc.getOffset() != null) {
            out.print(" offset ");
            lc.getOffset().accept(this, step + 1);
        }
        out.println();
        return null;
    }

    @Override
    public Void visit(FunctionDecl fd, Integer step) throws CompilationException {
        out.print(skip(step) + "declare function " + generateFullName(null, fd.getSignature().getName()) + "(");
        List<Identifier> parameters = new ArrayList<>();
        parameters.addAll(fd.getParamList());
        printDelimitedIdentifiers(parameters, COMMA);
        out.println(") {");
        fd.getFuncBody().accept(this, step + 2);
        out.println();
        out.print(skip(step) + "}");
        out.println(";");
        return null;
    }

    @Override
    public Void visit(UnaryExpr u, Integer step) throws CompilationException {
        out.print(u.getExprType() == UnaryExprType.NEGATIVE ? "-" : "");
        u.getExpr().accept(this, 0);
        return null;
    }

    @Override
    public Void visit(FieldAccessor fa, Integer step) throws CompilationException {
        fa.getExpr().accept(this, step + 1);
        out.print("." + normalize(fa.getIdent().getValue()));
        return null;
    }

    @Override
    public Void visit(IndexAccessor fa, Integer step) throws CompilationException {
        fa.getExpr().accept(this, step + 1);
        out.print("[");
        if (fa.isAny()) {
            out.print("?");
        } else {
            fa.getIndexExpr().accept(this, step + 1);
        }
        out.print("]");
        return null;
    }

    @Override
    public Void visit(TypeDecl t, Integer step) throws CompilationException {
        out.println(skip(step) + "create type " + generateFullName(t.getDataverseName(), t.getIdent())
                + generateIfNotExists(t.getIfNotExists()) + " as");
        t.getTypeDef().accept(this, step + 1);
        out.println();
        return null;
    }

    @Override
    public Void visit(TypeReferenceExpression t, Integer arg) throws CompilationException {
        if (t.getIdent().first != null && t.getIdent().first.getValue() != null) {
            out.print(normalize(t.getIdent().first.getValue()));
            out.print('.');
        }
        out.print(normalize(t.getIdent().second.getValue()));
        return null;
    }

    @Override
    public Void visit(RecordTypeDefinition r, Integer step) throws CompilationException {
        if (r.getRecordKind() == RecordKind.CLOSED) {
            out.print(" closed ");
        }

        out.println("{");
        Iterator<String> nameIter = r.getFieldNames().iterator();
        Iterator<TypeExpression> typeIter = r.getFieldTypes().iterator();
        Iterator<Boolean> isOptionalIter = r.getOptionableFields().iterator();
        boolean first = true;
        while (nameIter.hasNext()) {
            if (first) {
                first = false;
            } else {
                out.println(COMMA);
            }
            String name = normalize(nameIter.next());
            TypeExpression texp = typeIter.next();
            Boolean isNullable = isOptionalIter.next();
            out.print(skip(step) + name + " : ");
            texp.accept(this, step + 2);
            if (isNullable) {
                out.print("?");
            }
        }
        out.println();
        out.println(skip(step - 2) + "}");
        return null;
    }

    @Override
    public Void visit(OrderedListTypeDefinition x, Integer step) throws CompilationException {
        out.print("[");
        x.getItemTypeExpression().accept(this, step + 2);
        out.print("]");
        return null;
    }

    @Override
    public Void visit(UnorderedListTypeDefinition x, Integer step) throws CompilationException {
        out.print("{{");
        x.getItemTypeExpression().accept(this, step + 2);
        out.print("}}");
        return null;
    }

    @Override
    public Void visit(DatasetDecl dd, Integer step) throws CompilationException {
        if (dd.getDatasetType() == DatasetType.INTERNAL) {
            out.print(skip(step) + "create " + datasetSymbol + generateFullName(dd.getDataverse(), dd.getName())
                    + generateIfNotExists(dd.getIfNotExists()) + "(" + dd.getQualifiedTypeName() + ")"
                    + " primary key ");
            printDelimitedKeys(((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getPartitioningExprs(), ",");
            if (((InternalDetailsDecl) dd.getDatasetDetailsDecl()).isAutogenerated()) {
                out.print(" autogenerated ");
            }
        } else if (dd.getDatasetType() == DatasetType.EXTERNAL) {
            out.print(
                    skip(step) + "create external " + datasetSymbol + generateFullName(dd.getDataverse(), dd.getName())
                            + "(" + dd.getQualifiedTypeName() + ")" + generateIfNotExists(dd.getIfNotExists()));
            ExternalDetailsDecl externalDetails = (ExternalDetailsDecl) dd.getDatasetDetailsDecl();
            out.print(" using " + revertStringToQuoted(externalDetails.getAdapter()));
            printConfiguration(externalDetails.getProperties());
        }
        Identifier nodeGroupName = dd.getNodegroupName();
        if (nodeGroupName != null) {
            out.print(" on " + nodeGroupName.getValue());
        }
        Map<String, String> hints = dd.getHints();
        if (dd.getHints().size() > 0) {
            out.print(" hints ");
            printProperties(hints);
        }
        if (dd.getDatasetType() == DatasetType.INTERNAL) {
            List<String> filterField = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getFilterField();
            if (filterField != null && filterField.size() > 0) {
                out.print(" with filter on ");
                printNestField(filterField);
            }
        }
        if (dd.getWithObjectNode() != null) {
            out.print(" with ");
            out.print(dd.getWithObjectNode().toString());
        }
        out.println(SEMICOLON);
        out.println();
        return null;
    }

    @Override
    public Void visit(DataverseDecl dv, Integer step) throws CompilationException {
        out.println(skip(step) + "use " + dataverseSymbol + normalize(dv.getDataverseName().getValue()) + ";\n\n");
        return null;
    }

    @Override
    public Void visit(WriteStatement ws, Integer step) throws CompilationException {
        out.print(skip(step) + "write output to " + ws.getNcName() + ":" + revertStringToQuoted(ws.getFileName()));
        if (ws.getWriterClassName() != null) {
            out.print(" using " + ws.getWriterClassName());
        }
        out.println();
        return null;
    }

    @Override
    public Void visit(SetStatement ss, Integer step) throws CompilationException {
        out.println(skip(step) + "set " + revertStringToQuoted(ss.getPropName()) + " "
                + revertStringToQuoted(ss.getPropValue()) + ";\n");
        return null;
    }

    @Override
    public Void visit(DisconnectFeedStatement ss, Integer step) throws CompilationException {
        out.println(skip(step) + "disconnect " + FEED + generateFullName(ss.getDataverseName(), ss.getFeedName())
                + " from " + datasetSymbol + generateFullName(ss.getDataverseName(), ss.getDatasetName()) + ";");
        return null;
    }

    @Override
    public Void visit(NodegroupDecl ngd, Integer step) throws CompilationException {
        out.println(
                CREATE + " nodegroup " + ngd.getNodegroupName() + generateIfNotExists(ngd.getIfNotExists()) + " on ");
        out.print(skip(step + 2));
        printDelimitedIdentifiers(ngd.getNodeControllerNames(), COMMA + "\n" + skip(step + 2));
        out.println();
        out.println(skip(step) + SEMICOLON);
        return null;
    }

    @Override
    public Void visit(LoadStatement stmtLoad, Integer step) throws CompilationException {
        out.print(skip(step) + "load " + datasetSymbol
                + generateFullName(stmtLoad.getDataverseName(), stmtLoad.getDatasetName()) + " using "
                + revertStringToQuoted(stmtLoad.getAdapter()) + " ");
        printConfiguration(stmtLoad.getProperties());
        out.println(stmtLoad.dataIsAlreadySorted() ? " pre-sorted" + SEMICOLON : SEMICOLON);
        out.println();
        return null;
    }

    @Override
    public Void visit(DropDatasetStatement del, Integer step) throws CompilationException {
        out.println(
                skip(step) + "drop " + datasetSymbol + generateFullName(del.getDataverseName(), del.getDatasetName())
                        + generateIfExists(del.getIfExists()) + SEMICOLON);
        return null;
    }

    @Override
    public Void visit(InsertStatement insert, Integer step) throws CompilationException {
        out.print(skip(step) + "insert into " + datasetSymbol
                + generateFullName(insert.getDataverseName(), insert.getDatasetName()));
        out.print("(");
        insert.getQuery().accept(this, step + 2);
        out.print(")");
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(DeleteStatement del, Integer step) throws CompilationException {
        out.print(skip(step) + "delete ");
        del.getVariableExpr().accept(this, step + 2);
        out.println(
                skip(step) + " from " + datasetSymbol + generateFullName(del.getDataverseName(), del.getDatasetName()));
        if (del.getCondition() != null) {
            out.print(skip(step) + " where ");
            del.getCondition().accept(this, step + 2);
        }
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(UpdateStatement update, Integer step) throws CompilationException {
        out.println(skip(step) + "update ");
        update.getVariableExpr().accept(this, step + 2);
        out.print(" in ");
        update.getTarget().accept(this, step + 2);
        out.println();
        out.print(skip(step) + "where ");
        update.getCondition().accept(this, step + 2);
        out.println();
        out.print("(");
        for (UpdateClause updateClause : update.getUpdateClauses()) {
            updateClause.accept(this, step + 4);
            out.println();
        }
        out.print(")");
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(UpdateClause del, Integer step) throws CompilationException {
        if (del.hasSet()) {
            out.println(skip(step) + "set ");
            del.getTarget().accept(this, step + 2);
            out.print("=");
            del.getTarget().accept(this, step + 2);
        } else if (del.hasInsert()) {
            del.getInsertStatement().accept(this, step + 2);
        } else if (del.hasDelete()) {
            del.getDeleteStatement().accept(this, step + 2);
        } else if (del.hasUpdate()) {
            del.getUpdateStatement().accept(this, step + 2);
        } else if (del.hasIfElse()) {
            out.println();
            out.print(skip(step) + "if (");
            del.getCondition().accept(this, step);
            out.print(")");
            out.println();
            out.print(skip(step) + "then ");
            del.getIfBranch().accept(this, step);
            if (del.hasElse()) {
                out.println();
                out.print(skip(step) + "else");
                del.getElseBranch().accept(this, step);
            }
            out.println();
        }
        return null;
    }

    @Override
    public Void visit(CreateIndexStatement cis, Integer step) throws CompilationException {
        out.print(skip(step) + CREATE + " index ");
        out.print(normalize(cis.getIndexName().getValue()) + " ");
        out.print(generateIfNotExists(cis.getIfNotExists()));
        out.print(" on ");
        out.print(generateFullName(cis.getDataverseName(), cis.getDatasetName()));

        out.print(" (");
        List<Pair<List<String>, IndexedTypeExpression>> fieldExprs = cis.getFieldExprs();
        int index = 0;
        int size = fieldExprs.size();
        for (Pair<List<String>, IndexedTypeExpression> entry : fieldExprs) {
            printNestField(entry.first);
            IndexedTypeExpression typeExpr = entry.second;
            if (typeExpr != null) {
                out.print(":");
                typeExpr.getType().accept(this, step);
                if (typeExpr.isUnknownable()) {
                    out.print('?');
                }
            }
            if (++index < size) {
                out.print(",");
            }
        }
        out.print(") type ");
        out.print(generateIndexTypeString(cis.getIndexType()));
        if (cis.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX && cis.getGramLength() >= 0) {
            out.print(" (");
            out.print(cis.getGramLength());
            out.print(")");
        }
        if (cis.isEnforced()) {
            out.print(" enforced");
        }
        out.println(SEMICOLON);
        out.println();
        return null;
    }

    @Override
    public Void visit(CreateDataverseStatement del, Integer step) throws CompilationException {
        out.print(CREATE + dataverseSymbol);
        out.print(normalize(del.getDataverseName().getValue()));
        out.print(generateIfNotExists(del.getIfNotExists()));
        String format = del.getFormat();
        if (format != null && !format.equals(DEFAULT_DATAVERSE_FORMAT)) {
            out.print(" with format ");
            out.print("\"" + format + "\"");
        }
        out.println(SEMICOLON);
        out.println();
        return null;
    }

    @Override
    public Void visit(IndexDropStatement del, Integer step) throws CompilationException {
        out.print(skip(step) + "drop index ");
        out.print(generateFullName(del.getDataverseName(), del.getDatasetName()));
        out.print("." + del.getIndexName());
        out.println(generateIfExists(del.getIfExists()) + SEMICOLON);
        return null;
    }

    @Override
    public Void visit(NodeGroupDropStatement del, Integer step) throws CompilationException {
        out.print(skip(step) + "drop nodegroup ");
        out.print(del.getNodeGroupName());
        out.println(generateIfExists(del.getIfExists()) + SEMICOLON);
        return null;
    }

    @Override
    public Void visit(DataverseDropStatement del, Integer step) throws CompilationException {
        out.print(skip(step) + "drop " + dataverseSymbol);
        out.print(normalize(del.getDataverseName().getValue()));
        out.println(generateIfExists(del.getIfExists()) + SEMICOLON);
        return null;
    }

    @Override
    public Void visit(TypeDropStatement del, Integer step) throws CompilationException {
        out.print(skip(step) + "drop type ");
        out.print(generateFullName(del.getDataverseName(), del.getTypeName()));
        out.println(generateIfExists(del.getIfExists()) + SEMICOLON);
        return null;
    }

    @Override
    public Void visit(ConnectFeedStatement connectFeedStmt, Integer step) throws CompilationException {
        out.print(skip(step) + "connect " + FEED);
        out.print(generateFullName(connectFeedStmt.getDataverseName(), new Identifier(connectFeedStmt.getFeedName())));
        out.print(" to " + datasetSymbol
                + generateFullName(connectFeedStmt.getDataverseName(), connectFeedStmt.getDatasetName()));
        if (connectFeedStmt.getPolicy() != null) {
            out.print(" using policy " + revertStringToQuoted(connectFeedStmt.getPolicy()));
        }
        if (connectFeedStmt.getAppliedFunctions() != null) {
            out.print(" apply function " + connectFeedStmt.getAppliedFunctions());
        }
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(CreateFeedStatement cfs, Integer step) throws CompilationException {
        out.print(skip(step) + "create " + FEED);
        out.print(generateFullName(cfs.getDataverseName(), cfs.getFeedName()));
        out.print(generateIfNotExists(cfs.getIfNotExists()));
        if (cfs.getWithObjectNode() != null) {
            out.print(" with ");
            out.print(cfs.getWithObjectNode().toString());
        }
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(StartFeedStatement startFeedStatement, Integer step) throws CompilationException {
        out.print(skip(step) + "start " + FEED);
        out.print(generateFullName(startFeedStatement.getDataverseName(), startFeedStatement.getFeedName()));
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(StopFeedStatement stopFeedStatement, Integer step) throws CompilationException {
        out.print(skip(step) + "stop " + FEED);
        out.print(generateFullName(stopFeedStatement.getDataverseName(), stopFeedStatement.getFeedName()));
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(FeedDropStatement del, Integer step) throws CompilationException {
        out.print(skip(step) + "drop " + FEED);
        out.print(generateFullName(del.getDataverseName(), del.getFeedName()));
        out.println(generateIfExists(del.getIfExists()) + SEMICOLON);
        return null;
    }

    @Override
    public Void visit(FeedPolicyDropStatement dfs, Integer step) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(CreateFeedPolicyStatement cfps, Integer step) throws CompilationException {
        out.print(skip(step) + CREATE + "ingestion policy ");
        out.print(cfps.getPolicyName());
        out.print(generateIfNotExists(cfps.getIfNotExists()));
        out.print(" from ");
        String srcPolicyName = revertStringToQuoted(cfps.getSourcePolicyName());
        if (srcPolicyName != null) {
            out.print(" policy ");
            out.print(srcPolicyName + " ");
            printConfiguration(cfps.getProperties());
        } else {
            out.print(" path ");
            out.print(cfps.getSourcePolicyFile() + " ");
            printConfiguration(cfps.getProperties());
        }
        String desc = cfps.getDescription();
        if (cfps.getDescription() != null) {
            out.print(" definition ");
            out.print(revertStringToQuoted(desc));
        }
        out.println(SEMICOLON);
        out.println();
        return null;
    }

    @Override
    public Void visit(CreateFunctionStatement cfs, Integer step) throws CompilationException {
        out.print(skip(step) + CREATE + " function ");
        out.print(generateIfNotExists(cfs.getIfNotExists()));
        out.print(
                this.generateFullName(cfs.getFunctionSignature().getNamespace(), cfs.getFunctionSignature().getName()));
        out.print("(");
        printDelimitedStrings(cfs.getParamList(), COMMA);
        out.println(") {");
        out.println(cfs.getFunctionBody());
        out.println("}" + SEMICOLON);
        out.println();
        return null;
    }

    @Override
    public Void visit(FunctionDropStatement del, Integer step) throws CompilationException {
        out.print(skip(step) + "drop function ");
        FunctionSignature funcSignature = del.getFunctionSignature();
        out.print(funcSignature.toString());
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(CompactStatement del, Integer step) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(ListSliceExpression expression, Integer step) throws CompilationException {
        out.println(skip(step) + "ListSliceExpression [");
        expression.getExpr().accept(this, step + 1);
        out.print(skip(step + 1) + "Index: ");
        expression.getStartIndexExpression().accept(this, step + 1);
        out.println(skip(step) + ":");

        // End index expression can be null (optional)
        if (expression.hasEndExpression()) {
            expression.getEndIndexExpression().accept(this, step + 1);
        }
        out.println(skip(step) + "]");
        return null;
    }

    protected void printConfiguration(Map<String, String> properties) {
        if (properties.size() > 0) {
            out.print("(");
            int index = 0;
            int size = properties.size();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                out.print("(" + revertStringToQuoted(entry.getKey()) + "=" + revertStringToQuoted(entry.getValue())
                        + ")");
                if (++index < size) {
                    out.print(COMMA);
                }
            }
            out.print(")");
        }
    }

    protected void printProperties(Map<String, String> properties) {
        if (properties.size() > 0) {
            out.print("(");
            int index = 0;
            int size = properties.size();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                out.print(revertStringToQuoted(entry.getKey()) + "=" + revertStringToQuoted(entry.getValue()));
                if (++index < size) {
                    out.print(COMMA);
                }
            }
            out.print(")");
        }
    }

    protected void printNestField(List<String> filterField) {
        printDelimitedStrings(filterField, ".");
    }

    protected void printDelimitedGbyExpressions(List<GbyVariableExpressionPair> gbyList, int step)
            throws CompilationException {
        int gbySize = gbyList.size();
        int gbyIndex = 0;
        for (GbyVariableExpressionPair pair : gbyList) {
            if (pair.getVar() != null) {
                pair.getVar().accept(this, step);
                out.print(assignSymbol);
            }
            pair.getExpr().accept(this, step);
            if (++gbyIndex < gbySize) {
                out.print(COMMA);
            }
        }
    }

    protected void printDelimitedObyExpressions(List<Expression> list, List<OrderModifier> mlist, Integer step)
            throws CompilationException {
        int index = 0;
        int size = list.size();
        for (Expression expr : list) {
            expr.accept(this, step);
            OrderModifier orderModifier = mlist.get(index);
            if (orderModifier != OrderModifier.ASC) {
                out.print(orderModifier.toString().toLowerCase());
            }
            if (++index < size) {
                out.print(COMMA);
            }
        }
    }

    protected void printDelimitedExpressions(List<? extends Expression> exprs, String delimiter, int step)
            throws CompilationException {
        int index = 0;
        int size = exprs.size();
        for (Expression expr : exprs) {
            expr.accept(this, step);
            if (++index < size) {
                out.print(delimiter);
            }
        }
    }

    protected void printDelimitedStrings(List<String> strs, String delimiter) {
        int index = 0;
        int size = strs.size();
        for (String str : strs) {
            out.print(normalize(str));
            if (++index < size) {
                out.print(delimiter);
            }
        }
    }

    protected void printDelimitedKeys(List<List<String>> keys, String delimiter) {
        int index = 0;
        int size = keys.size();
        for (List<String> strs : keys) {
            printDelimitedStrings(strs, ".");
            if (++index < size) {
                out.print(delimiter);
            }
        }
    }

    protected void printDelimitedIdentifiers(List<Identifier> ids, String delimiter) {
        int index = 0;
        int size = ids.size();
        for (Identifier id : ids) {
            out.print(normalize(id.getValue()));
            if (++index < size) {
                out.print(delimiter);
            }
        }
    }

    protected boolean needQuotes(String str) {
        if (str.length() == 0) {
            return false;
        }
        if (!validIdentifierStartChars.contains(str.charAt(0))) {
            return true;
        }
        for (char ch : str.toCharArray()) {
            if (!validIdentifierChars.contains(ch)) {
                return true;
            }
        }
        return false;
    }

    protected String normalize(String str) {
        if (needQuotes(str)) {
            return revertStringToQuoted(str);
        }
        return str;
    }

    protected String generateFullName(String namespace, String identifier) {
        String dataversePrefix = namespace != null && !namespace.equals("Metadata") ? normalize(namespace) + "." : "";
        return dataversePrefix + normalize(identifier);
    }

    protected String generateFullName(Identifier dv, Identifier ds) {
        String dataverse = dv != null ? dv.getValue() : null;
        return generateFullName(dataverse, ds.getValue());
    }

    protected String generateIfNotExists(boolean ifNotExits) {
        return ifNotExits ? " if not exists " : "";
    }

    protected String generateIfExists(boolean ifExits) {
        return ifExits ? " if exists" : "";
    }

    protected String generateIndexTypeString(IndexType type) {
        switch (type) {
            case BTREE:
                return "btree";
            case RTREE:
                return "rtree";
            case SINGLE_PARTITION_WORD_INVIX:
                return "fulltext";
            case LENGTH_PARTITIONED_WORD_INVIX:
                return "keyword";
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                return "ngram";
            default:
                return "";
        }
    }

    public String revertStringToQuoted(String inputStr) {
        int pos = 0;
        int size = inputStr.length();
        StringBuffer result = new StringBuffer();
        for (; pos < size; ++pos) {
            char ch = inputStr.charAt(pos);
            switch (ch) {
                case '\\':
                    result.append("\\\\");
                    break;
                case '\b':
                    result.append("\\b");
                    break;
                case '\f':
                    result.append("\\f");
                    break;
                case '\n':
                    result.append("\\n");
                    break;
                case '\r':
                    result.append("\\r");
                    break;
                case '\t':
                    result.append("\\t");
                    break;
                case '\'':
                    result.append("\\\'");
                    break;
                case '\"':
                    result.append("\\\"");
                    break;
                default:
                    result.append(ch);
            }
        }
        return "\"" + result.toString() + "\"";
    }

    public String revertStringToLiteral(String inputStr) {
        int pos = 0;
        int size = inputStr.length();
        StringBuffer result = new StringBuffer();
        for (; pos < size; ++pos) {
            char ch = inputStr.charAt(pos);
            switch (ch) {
                case '\\':
                    result.append("\\\\");
                    break;
                case '\b':
                    result.append("\\b");
                    break;
                case '\f':
                    result.append("\\f");
                    break;
                case '\n':
                    result.append("\\n");
                    break;
                case '\r':
                    result.append("\\r");
                    break;
                case '\t':
                    result.append("\\t");
                    break;
                case '\'':
                    result.append("\\\'");
                    break;
                default:
                    result.append(ch);
            }
        }
        return "\'" + result.toString() + "\'";
    }

    protected void printHints(List<IExpressionAnnotation> annotations, int step) {
        if (annotations != null) {
            for (IExpressionAnnotation annotation : annotations) {
                out.print(" /*+ " + annotation + " */ ");
            }
        }
    }
}
