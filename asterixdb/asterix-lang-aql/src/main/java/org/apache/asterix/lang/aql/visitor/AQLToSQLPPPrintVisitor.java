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
package org.apache.asterix.lang.aql.visitor;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.aql.util.AQLVariableSubstitutionUtil;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Clause.ClauseType;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.FormatPrintVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class AQLToSQLPPPrintVisitor extends FormatPrintVisitor implements IAQLVisitor<Void, Integer> {

    private final PrintWriter out;
    private final Set<String> reservedKeywords = new HashSet<String>();
    private int generatedId = 0;

    public AQLToSQLPPPrintVisitor() {
        this(new PrintWriter(System.out));
    }

    public AQLToSQLPPPrintVisitor(PrintWriter out) {
        super(out);
        this.out = out;
        initialize();
    }

    private void initialize() {
        dataverseSymbol = " database ";
        datasetSymbol = " table ";
        assignSymbol = "=";
        reservedKeywords.addAll(Arrays.asList(new String[] { "order", "value", "nest", "keyword", "all" }));
    }

    @Override
    public Void visit(FLWOGRExpression flwor, Integer step) throws CompilationException {
        if (step != 0) {
            out.println("(");
        }
        List<Clause> clauseList = new ArrayList<Clause>();
        clauseList.addAll(flwor.getClauseList());

        // Processes data-independent let clauses.
        if (hasFor(clauseList)) {
            processLeadingLetClauses(step, clauseList);
        }

        // Distill unnecessary order-bys before a group-by.
        distillRedundantOrderby(clauseList);

        // Correlated "for" clauses after group-by.
        Pair<GroupbyClause, List<Clause>> extraction = extractUnnestAfterGroupby(clauseList);
        GroupbyClause cuttingGbyClause = extraction.first;
        List<Clause> unnestClauseList = extraction.second;
        Expression returnExpr = flwor.getReturnExpr();
        if (unnestClauseList.size() == 0) {
            if (hasFor(clauseList)) {
                out.print(skip(step) + "select element ");
                returnExpr.accept(this, step + 2);
                out.println();
            } else {
                // The FLOWGR only contains let-return, then inline let binding expressions into the return expression.
                Map<VariableExpr, Expression> varExprMap = extractLetBindingVariables(clauseList, cuttingGbyClause);
                returnExpr = (Expression) AQLVariableSubstitutionUtil.substituteVariable(returnExpr, varExprMap);
                returnExpr.accept(this, step);
                return null;
            }
        }

        String generated = generateVariableSymbol();
        if (unnestClauseList.size() > 0) {
            Map<VariableExpr, Expression> varExprMap =
                    extractDefinedCollectionVariables(clauseList, cuttingGbyClause, generated);

            returnExpr = (Expression) AQLVariableSubstitutionUtil.substituteVariable(returnExpr, varExprMap);
            List<Clause> newUnnestClauses = new ArrayList<Clause>();
            for (Clause nestedCl : unnestClauseList) {
                newUnnestClauses.add((Clause) AQLVariableSubstitutionUtil.substituteVariable(nestedCl, varExprMap));
            }
            unnestClauseList = newUnnestClauses;

            out.print(skip(step) + "select element " + (hasDistinct(unnestClauseList) ? "distinct " : ""));
            returnExpr.accept(this, step + 2);
            out.println();
            out.println(skip(step) + "from");
            out.print(skip(step + 2) + "( select element " + (hasDistinct(clauseList) ? "distinct " : "") + "{");
            int index = 0;
            int size = varExprMap.size();
            for (VariableExpr var : varExprMap.keySet()) {
                out.print("\'" + var.getVar().getValue().substring(1) + "\':" + var.getVar().getValue().substring(1));
                if (++index < size) {
                    out.print(COMMA);
                }
            }
            out.println("}");
        }

        reorder(clauseList);
        reorder(unnestClauseList);

        mergeConsecutiveWhereClauses(clauseList);
        mergeConsecutiveWhereClauses(unnestClauseList);

        boolean firstFor = true;
        boolean firstLet = true;
        int forStep = unnestClauseList.size() == 0 ? step : step + 3;
        int size = clauseList.size();
        // Processes all other clauses, with special printing for consecutive
        // "for"s.
        for (int i = 0; i < size; ++i) {
            Clause cl = clauseList.get(i);
            if (cl.getClauseType() == ClauseType.FOR_CLAUSE) {
                boolean hasConsequentFor = false;
                if (i < size - 1) {
                    Clause nextCl = clauseList.get(i + 1);
                    hasConsequentFor = nextCl.getClauseType() == ClauseType.FOR_CLAUSE;
                }
                visitForClause((ForClause) cl, forStep, firstFor, hasConsequentFor);
                firstFor = false;
            } else if (cl.getClauseType() == ClauseType.LET_CLAUSE) {
                boolean hasConsequentLet = false;
                if (i < size - 1) {
                    Clause nextCl = clauseList.get(i + 1);
                    hasConsequentLet = nextCl.getClauseType() == ClauseType.LET_CLAUSE;
                }
                visitLetClause((LetClause) cl, forStep, firstLet, hasConsequentLet);
                firstLet = false;
            } else {
                cl.accept(this, forStep);
            }

            if (cl.getClauseType() == ClauseType.FROM_CLAUSE || cl.getClauseType() == ClauseType.GROUP_BY_CLAUSE) {
                firstLet = true;
            }
        }

        if (unnestClauseList.size() > 0) {
            out.println(skip(forStep - 1) + ") as " + generated.substring(1) + ",");
            for (Clause nestedCl : unnestClauseList) {
                if (nestedCl.getClauseType() == ClauseType.FOR_CLAUSE) {
                    visitForClause((ForClause) nestedCl, step - 1, firstFor, false);
                } else {
                    nestedCl.accept(this, step);
                }
            }
        }

        if (step > 0) {
            out.print(skip(step - 2) + ")");
        }
        return null;
    }

    @Override
    public Void visit(ForClause fc, Integer step) throws CompilationException {
        // The processing of a "for" clause depends on its neighbor clauses,
        // hence the logic goes to visit(FLWOGRExpression).
        return null;
    }

    private void visitForClause(ForClause fc, Integer step, boolean startFor, boolean hasConsequentFor)
            throws CompilationException {
        if (startFor) {
            out.print(skip(step) + "from  ");
        } else {
            out.print(skip(step + 3));
        }
        fc.getInExpr().accept(this, step + 2);
        out.print(" as ");
        fc.getVarExpr().accept(this, step + 2);
        if (fc.hasPosVar()) {
            out.print(" at ");
            fc.getPosVarExpr().accept(this, step + 2);
        }
        if (hasConsequentFor) {
            out.print(COMMA);
        }
        out.println();
    }

    private void visitLetClause(LetClause lc, Integer step, boolean startLet, boolean hasConsequentLet)
            throws CompilationException {
        if (startLet) {
            out.print(skip(step) + "with  ");
        } else {
            out.print(skip(step + 3));
        }
        lc.getVarExpr().accept(this, step + 3);
        out.print(" as ");
        lc.getBindingExpr().accept(this, step + 3);
        if (hasConsequentLet) {
            out.print(COMMA);
        }
        out.println();
    }

    @Override
    public Void visit(Query q, Integer step) throws CompilationException {
        Expression expr = q.getBody();
        if (expr != null) {
            if (expr.getKind() != Kind.FLWOGR_EXPRESSION) {
                out.print("select element ");
                expr.accept(this, step + 2);
            } else {
                expr.accept(this, step);
            }
        }
        if (q.isTopLevel()) {
            out.println(SEMICOLON);
        }
        return null;
    }

    @Override
    public Void visit(DataverseDecl dv, Integer step) throws CompilationException {
        out.println(skip(step) + "use " + normalize(dv.getDataverseName().getValue()) + ";\n\n");
        return null;
    }

    @Override
    public Void visit(UnionExpr u, Integer step) throws CompilationException {
        printDelimitedExpressions(u.getExprs(), "\n" + skip(step) + "union\n" + skip(step), step);
        return null;
    }

    @Override
    public Void visit(DistinctClause dc, Integer step) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(VariableExpr v, Integer step) {
        String varStr = v.getVar().getValue().substring(1);
        if (reservedKeywords.contains(varStr)) {
            varStr = varStr + "s";
        }
        out.print(varStr);
        return null;
    }

    @Override
    public Void visit(LetClause lc, Integer step) throws CompilationException {
        out.print(skip(step) + "with ");
        lc.getVarExpr().accept(this, step + 2);
        out.print(" as ");
        Expression bindingExpr = lc.getBindingExpr();
        bindingExpr.accept(this, step + 2);
        out.println();
        return null;
    }

    @Override
    public Void visit(CallExpr callExpr, Integer step) throws CompilationException {
        FunctionSignature signature = callExpr.getFunctionSignature();
        if (signature.getNamespace() != null && signature.getNamespace().equals("Metadata")
                && signature.getName().equals("dataset") && signature.getArity() == 1) {
            LiteralExpr expr = (LiteralExpr) callExpr.getExprList().get(0);
            out.print(normalize(expr.getValue().getStringValue()));
        } else {
            printHints(callExpr.getHints(), step);
            out.print(generateFullName(callExpr.getFunctionSignature().getNamespace(),
                    callExpr.getFunctionSignature().getName()) + "(");
            printDelimitedExpressions(callExpr.getExprList(), COMMA, step);
            out.print(")");
        }
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Integer step) throws CompilationException {
        if (gc.hasHashGroupByHint()) {
            out.println(skip(step) + "/* +hash */");
        }
        out.print(skip(step) + "group by ");
        printDelimitedGbyExpressions(gc.getGbyPairList(), step + 2);
        out.println();
        return null;
    }

    @Override
    public Void visit(InsertStatement insert, Integer step) throws CompilationException {
        out.print(skip(step) + "insert into " + generateFullName(insert.getDataverseName(), insert.getDatasetName())
                + "\n");
        insert.getQuery().accept(this, step);
        out.println(SEMICOLON);
        return null;
    }

    @Override
    public Void visit(DeleteStatement del, Integer step) throws CompilationException {
        out.print(skip(step) + "delete ");
        del.getVariableExpr().accept(this, step + 2);
        out.println(skip(step) + " from " + generateFullName(del.getDataverseName(), del.getDatasetName()));
        if (del.getCondition() != null) {
            out.print(skip(step) + " where ");
            del.getCondition().accept(this, step + 2);
        }
        out.println(SEMICOLON);
        return null;
    }

    @Override
    protected String normalize(String str) {
        if (needQuotes(str) || containsReservedKeyWord(str.toLowerCase())) {
            return revertStringToQuoted(str);
        }
        return str;
    }

    protected boolean containsReservedKeyWord(String str) {
        if (reservedKeywords.contains(str)) {
            return true;
        }
        return false;
    }

    @Override
    protected void printDelimitedGbyExpressions(List<GbyVariableExpressionPair> gbyList, int step)
            throws CompilationException {
        int gbySize = gbyList.size();
        int gbyIndex = 0;
        for (GbyVariableExpressionPair pair : gbyList) {
            pair.getExpr().accept(this, step);
            if (pair.getVar() != null) {
                out.print(" as ");
                pair.getVar().accept(this, step);
            }
            if (++gbyIndex < gbySize) {
                out.print(COMMA);
            }
        }
    }

    @Override
    protected void printDelimitedIdentifiers(List<Identifier> ids, String delimiter) {
        int index = 0;
        int size = ids.size();
        for (Identifier id : ids) {
            String idStr = id.getValue();
            if (idStr.startsWith("$")) {
                id = new Identifier(idStr.substring(1));
            }
            out.print(id);
            if (++index < size) {
                out.print(delimiter);
            }
        }
    }

    // Collects produced variables from group-by.
    private List<VariableExpr> collectProducedVariablesFromGroupby(GroupbyClause gbyClause) {
        List<VariableExpr> producedVars = new ArrayList<VariableExpr>();
        for (GbyVariableExpressionPair keyPair : gbyClause.getGbyPairList()) {
            producedVars.add(keyPair.getVar());
        }
        if (gbyClause.hasDecorList()) {
            for (GbyVariableExpressionPair keyPair : gbyClause.getDecorPairList()) {
                producedVars.add(keyPair.getVar());
            }
        }
        if (gbyClause.hasWithMap()) {
            producedVars.addAll(gbyClause.getWithVarMap().values());
        }
        return producedVars;
    }

    // Randomly generates a new variable symbol.
    private String generateVariableSymbol() {
        return "$gen" + generatedId++;
    }

    // Removes all redundant order by clauses.
    private void distillRedundantOrderby(List<Clause> clauseList) {
        List<Clause> redundantOrderbys = new ArrayList<Clause>();
        boolean gbyAfterOrderby = false;
        for (Clause cl : clauseList) {
            if (cl.getClauseType() == ClauseType.ORDER_BY_CLAUSE) {
                redundantOrderbys.add(cl);
            }
            if (cl.getClauseType() == ClauseType.GROUP_BY_CLAUSE) {
                gbyAfterOrderby = true;
                break;
            }
        }
        if (gbyAfterOrderby) {
            clauseList.removeAll(redundantOrderbys);
        }

        redundantOrderbys.clear();
        for (Clause cl : clauseList) {
            if (cl.getClauseType() == ClauseType.ORDER_BY_CLAUSE) {
                redundantOrderbys.add(cl);
            }
        }
        if (redundantOrderbys.size() > 0) {
            redundantOrderbys.remove(redundantOrderbys.size() - 1);
        }
        clauseList.removeAll(redundantOrderbys);
    }

    // Processes leading "let"s in a FLWOGR.
    private void processLeadingLetClauses(Integer step, List<Clause> clauseList) throws CompilationException {
        List<Clause> processedLetList = new ArrayList<Clause>();
        boolean firstLet = true;
        int size = clauseList.size();
        for (int i = 0; i < size; ++i) {
            Clause cl = clauseList.get(i);
            if (cl.getClauseType() != ClauseType.LET_CLAUSE) {
                break;
            }
            boolean hasConsequentLet = false;
            if (i < size - 1) {
                Clause nextCl = clauseList.get(i + 1);
                hasConsequentLet = nextCl.getClauseType() == ClauseType.LET_CLAUSE;
            }
            visitLetClause((LetClause) cl, step, firstLet, hasConsequentLet);
            firstLet = false;
            processedLetList.add(cl);
        }
        clauseList.removeAll(processedLetList);
    }

    // Extracts all clauses that led by a "for" clause after the first group-by
    // clause in the input clause list.
    // Those extracted clauses will be removed from the input clause list.
    /**
     * @param clauseList
     *            , a list of clauses
     * @return the cutting group-by clause and the list of extracted clauses.
     * @throws CompilationException
     */
    private Pair<GroupbyClause, List<Clause>> extractUnnestAfterGroupby(List<Clause> clauseList)
            throws CompilationException {
        List<Clause> nestedClauses = new ArrayList<Clause>();
        GroupbyClause cuttingGbyClause = null;
        boolean meetGroupBy = false;
        boolean nestedClauseStarted = false;
        for (Clause cl : clauseList) {
            if (cl.getClauseType() == ClauseType.GROUP_BY_CLAUSE) {
                meetGroupBy = true;
                cuttingGbyClause = (GroupbyClause) cl;
                continue;
            }
            if (meetGroupBy && cl.getClauseType() == ClauseType.FOR_CLAUSE) {
                nestedClauseStarted = true;
            }
            if (nestedClauseStarted) {
                nestedClauses.add(cl);
            }
        }
        clauseList.removeAll(nestedClauses);
        return new Pair<GroupbyClause, List<Clause>>(cuttingGbyClause, nestedClauses);
    }

    // Extracts the variables to be substituted with a path access.
    private Map<VariableExpr, Expression> extractDefinedCollectionVariables(List<Clause> clauses,
            GroupbyClause cuttingGbyClause, String generatedAlias) {
        Map<VariableExpr, Expression> varExprMap = new HashMap<VariableExpr, Expression>();
        List<VariableExpr> varToSubstitute = collectProducedVariablesFromGroupby(cuttingGbyClause);
        int gbyIndex = clauses.indexOf(cuttingGbyClause);
        for (int i = gbyIndex + 1; i < clauses.size(); i++) {
            Clause cl = clauses.get(i);
            if (cl.getClauseType() == ClauseType.LET_CLAUSE) {
                varToSubstitute.add(((LetClause) cl).getVarExpr());
            }
        }
        for (VariableExpr var : varToSubstitute) {
            varExprMap.put(var, new FieldAccessor(new VariableExpr(new VarIdentifier(generatedAlias)),
                    new VarIdentifier(var.getVar().getValue().substring(1))));
        }
        return varExprMap;
    }

    // Extracts the variables to be substituted.
    private Map<VariableExpr, Expression> extractLetBindingVariables(List<Clause> clauses,
            GroupbyClause cuttingGbyClause) throws CompilationException {
        Map<VariableExpr, Expression> varExprMap = new HashMap<VariableExpr, Expression>();
        int gbyIndex = clauses.indexOf(cuttingGbyClause);
        for (int i = gbyIndex + 1; i < clauses.size(); i++) {
            Clause cl = clauses.get(i);
            if (cl.getClauseType() == ClauseType.LET_CLAUSE) {
                LetClause letClause = (LetClause) cl;
                // inline let variables one by one iteratively.
                letClause.setBindingExpr((Expression) AQLVariableSubstitutionUtil
                        .substituteVariable(letClause.getBindingExpr(), varExprMap));
                varExprMap.put(letClause.getVarExpr(), letClause.getBindingExpr());
            }
        }
        return varExprMap;
    }

    // Re-order clauses.
    private List<Clause> reorder(List<Clause> clauses) {
        Comparator<Clause> comparator = new ClauseComparator();
        List<Clause> results = new ArrayList<Clause>();
        int size = clauses.size();
        int start = 0;
        for (int index = 0; index < size; ++index) {
            Clause clause = clauses.get(index);
            if (clause.getClauseType() == ClauseType.GROUP_BY_CLAUSE) {
                List<Clause> subList = clauses.subList(start, index);
                Collections.sort(subList, comparator);
                results.addAll(subList);
                results.add(clause);
                start = index + 1;
            }
        }
        if (start < clauses.size()) {
            List<Clause> subList = clauses.subList(start, size);
            Collections.sort(subList, comparator);
            results.addAll(subList);
        }
        return results;
    }

    // Merge consecutive "where" clauses.
    private void mergeConsecutiveWhereClauses(List<Clause> clauses) throws CompilationException {
        List<Clause> results = new ArrayList<Clause>();
        int size = clauses.size();
        for (int index = 0; index < size;) {
            Clause clause = clauses.get(index);
            if (clause.getClauseType() != ClauseType.WHERE_CLAUSE) {
                results.add(clause);
                ++index;
            } else {
                List<Expression> expressions = new ArrayList<Expression>();
                Clause firstWhereClause = clause;
                do {
                    WhereClause whereClause = (WhereClause) clause;
                    expressions.add(whereClause.getWhereExpr());
                    if (++index >= size) {
                        break;
                    }
                    clause = clauses.get(index);
                } while (clause.getClauseType() == ClauseType.WHERE_CLAUSE);
                if (expressions.size() > 1) {
                    OperatorExpr newWhereExpr = new OperatorExpr();
                    newWhereExpr.setExprList(expressions);
                    newWhereExpr.setCurrentop(true);
                    for (int operatorIndex = 0; operatorIndex < expressions.size(); ++operatorIndex) {
                        newWhereExpr.addOperator(OperatorType.AND);
                    }
                    results.add(new WhereClause(newWhereExpr));
                } else {
                    results.add(firstWhereClause);
                }
            }
        }
        clauses.clear();
        clauses.addAll(results);
    }

    // Where there is a distinct clause.
    protected boolean hasDistinct(List<Clause> clauses) {
        for (Clause clause : clauses) {
            if (clause.getClauseType() == ClauseType.DISTINCT_BY_CLAUSE) {
                return true;
            }
        }
        return false;
    }

    // Whether the list of clauses contains a for clause.
    private boolean hasFor(List<Clause> clauses) {
        for (Clause cl : clauses) {
            if (cl.getClauseType() == ClauseType.FOR_CLAUSE) {
                return true;
            }
        }
        return false;
    }
}

/**
 * This comparator is used to safely mutate the order of clauses in a FLWOGR
 * expression. Note: clauses before and after a group-by cannot be re-aligned.
 */
class ClauseComparator implements Comparator<Clause> {

    @Override
    public int compare(Clause left, Clause right) {
        int ordinalLeft = left.getClauseType().ordinal();
        int ordinalRight = right.getClauseType().ordinal();
        return ordinalLeft > ordinalRight ? 1 : (ordinalLeft == ordinalRight ? 0 : -1);
    }
}
