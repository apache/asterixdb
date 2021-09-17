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
package org.apache.asterix.test.array;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

public class ArrayQuery {
    private final List<FromExpr> fromExprs = new ArrayList<>();
    private final List<UnnestStep> unnestSteps = new ArrayList<>();
    private final List<JoinStep> joinSteps = new ArrayList<>();
    private final List<SelectExpr> selectExprs = new ArrayList<>();
    private final List<Conjunct> whereConjuncts = new ArrayList<>();
    private String queryString;

    @Override
    public String toString() {
        return queryString;
    }

    public List<FromExpr> getFromExprs() {
        return fromExprs;
    }

    public List<UnnestStep> getUnnestSteps() {
        return unnestSteps;
    }

    public List<JoinStep> getJoinSteps() {
        return joinSteps;
    }

    public List<SelectExpr> getSelectExprs() {
        return selectExprs;
    }

    public List<Conjunct> getWhereConjuncts() {
        return whereConjuncts;
    }

    public static class FromExpr {
        final String datasetName;
        final String alias;

        public FromExpr(String datasetName, String alias) {
            this.datasetName = datasetName;
            this.alias = alias;
        }

        @Override
        public String toString() {
            return String.format("%s AS %s", datasetName, alias);
        }
    }

    public static class UnnestStep {
        final String sourceAlias;
        final String arrayField;
        final String alias;

        public UnnestStep(String sourceAlias, String arrayField, String alias) {
            this.sourceAlias = sourceAlias;
            this.arrayField = arrayField;
            this.alias = alias;
        }

        public String getNamedExpr() {
            return sourceAlias + "." + arrayField;
        }

        @Override
        public String toString() {
            return String.format("UNNEST %s AS %s", getNamedExpr(), alias);
        }
    }

    public static class SelectExpr {
        final String expr;
        final String alias;

        public SelectExpr(String expr, String alias) {
            this.expr = expr;
            this.alias = alias;
        }

        public String asKeyValueString() {
            return String.format("\"%s\":%s", alias, expr);
        }

        @Override
        public String toString() {
            return String.format("%s AS %s", expr, alias);
        }
    }

    public interface Conjunct {
        // Note: this is just a marker interface.
        String toString();
    }

    public static class SimpleConjunct implements Conjunct {
        final String expressionOne;
        final String expressionTwo;
        final String expressionThree;
        final String operator;
        final String annotation;

        public SimpleConjunct(String e1, String e2, String e3, String operator, String annotation) {
            this.expressionOne = e1;
            this.expressionTwo = e2;
            this.expressionThree = e3;
            this.operator = operator;
            this.annotation = annotation;
        }

        public SimpleConjunct(String e1, String e2, String operator, String annotation) {
            this.expressionOne = e1;
            this.expressionTwo = e2;
            this.expressionThree = null;
            this.operator = operator;
            this.annotation = annotation;
        }

        @Override
        public String toString() {
            String a = (annotation == null) ? "" : (" " + annotation);
            if (operator.equals("BETWEEN")) {
                return String.format("( %s%s BETWEEN %s AND %s )", expressionOne, a, expressionTwo, expressionThree);

            } else {
                return String.format("( %s%s %s %s )", expressionOne, a, operator, expressionTwo);
            }
        }
    }

    public static class QuantifiedConjunct implements Conjunct {
        final List<String> arraysToQuantify;
        final List<String> quantifiedVars;
        final List<Conjunct> conjuncts;
        final String quantificationType;

        public QuantifiedConjunct(List<String> arraysToQuantify, List<String> quantifiedVars, List<Conjunct> conjuncts,
                String quantificationType) {
            this.arraysToQuantify = arraysToQuantify;
            this.quantifiedVars = quantifiedVars;
            this.conjuncts = conjuncts;
            this.quantificationType = quantificationType;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < arraysToQuantify.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(quantifiedVars.get(i)).append(" IN ").append(arraysToQuantify.get(i));
            }
            String quantifiedBuildExpr = sb.toString();

            return String.format("( %s %s SATISFIES %s )", quantificationType, quantifiedBuildExpr,
                    conjuncts.stream().map(Conjunct::toString).collect(Collectors.joining(" AND ")));
        }
    }

    public static class ExistsConjunct implements Conjunct {
        final FromExpr fromExpr;
        final List<UnnestStep> unnestSteps;
        final List<SimpleConjunct> conjuncts;

        public ExistsConjunct(FromExpr fromExpr, List<UnnestStep> unnestSteps, List<SimpleConjunct> conjuncts) {
            this.fromExpr = fromExpr;
            this.unnestSteps = unnestSteps;
            this.conjuncts = conjuncts;
        }

        @Override
        public String toString() {
            String unnestClause = unnestSteps.isEmpty() ? ""
                    : unnestSteps.stream().map(UnnestStep::toString).collect(Collectors.joining(" ")) + " ";
            return String.format("EXISTS ( FROM %s %sWHERE %s SELECT 1 )", fromExpr.toString(), unnestClause,
                    conjuncts.stream().map(Conjunct::toString).collect(Collectors.joining(" AND ")));
        }
    }

    public static class JoinStep {
        final FromExpr subqueryFromExpr;
        final List<UnnestStep> subqueryUnnestSteps;
        final List<SelectExpr> subquerySelectExprs;
        final List<SimpleConjunct> onExprConjuncts;
        final String joinType;
        final String joinAlias;

        public JoinStep(List<UnnestStep> subqueryUnnestSteps, List<SelectExpr> subquerySelectExprs,
                FromExpr subqueryFromExpr, List<SimpleConjunct> onExprConjuncts, String joinType, String joinAlias) {
            this.subqueryUnnestSteps = subqueryUnnestSteps;
            this.subquerySelectExprs = subquerySelectExprs;
            this.subqueryFromExpr = subqueryFromExpr;
            this.onExprConjuncts = onExprConjuncts;
            this.joinType = joinType;
            this.joinAlias = joinAlias;
        }

        public String getJoinExpr() {
            String keyValuePairs = subquerySelectExprs.stream().map(SelectExpr::asKeyValueString)
                    .collect(Collectors.joining(",\n\t\t"));
            return String.format("(\n\tFROM %s\n\t%s\n\tSELECT VALUE { \n\t\t%s\n\t} )", subqueryFromExpr,
                    subqueryUnnestSteps.stream().map(UnnestStep::toString).collect(Collectors.joining("\n\t")),
                    keyValuePairs);
        }

        @Override
        public String toString() {
            String onExpr = onExprConjuncts.stream().map(SimpleConjunct::toString).collect(Collectors.joining(" AND "));
            return String.format("%s JOIN %s AS %s\nON %s", joinType, getJoinExpr(), joinAlias, onExpr);
        }
    }

    @SuppressWarnings("rawtypes")
    public static class Builder {
        public static abstract class ValueSupplier {
            public abstract List<UnnestStep> getExtraUnnestSteps(String alias);

            public abstract String getJoinType();

            public abstract boolean getIsConditionOnUnnest();

            public abstract boolean getIsConstantConditionWithJoins();

            public abstract boolean getIsCrossProductOnProbes();

            public abstract String getOperatorForJoin();

            public abstract String getOperatorForConstant();

            public abstract Pair<String, String> getRangeFromDomain(BaseWisconsinTable.Domain domain);

            public abstract boolean getIsBetweenConjunct();

            public abstract String getQuantifier();

            public abstract List<Conjunct> getExtraConjuncts(String alias);

            public abstract List<Conjunct> getExtraQuantifiersAndConjuncts(List<String> arraysToQuantify,
                    List<String> quantifiedVars, String alias);

            public abstract boolean getIsUseNestedQuantification();
        }

        private int limitCount;
        private int unnestStepDepth;
        private int numberOfJoins;
        private int numberOfExplicitJoins;

        private final ArrayIndex arrayIndex;
        private final ArrayQuery arrayQuery;
        private String workingUnnestAlias;
        private ValueSupplier valueSupplier;

        public Builder(ArrayIndex arrayIndex) {
            this.arrayQuery = new ArrayQuery();
            this.arrayIndex = arrayIndex;
        }

        public void setUnnestStepDepth(int unnestStepDepth) {
            this.unnestStepDepth = unnestStepDepth;
        }

        public void setNumberOfJoins(int numberOfJoins) {
            this.numberOfJoins = numberOfJoins;
        }

        public void setNumberOfExplicitJoins(int numberOfExplicitJoins) {
            this.numberOfExplicitJoins = numberOfExplicitJoins;
        }

        @SuppressWarnings("SameParameterValue")
        public void setLimitCount(int limitCount) {
            this.limitCount = limitCount;
        }

        public void setValueSupplier(ValueSupplier valueSupplier) {
            this.valueSupplier = valueSupplier;
        }

        public ArrayQuery build() {
            StringBuilder sb = new StringBuilder();

            // Start with our FROM clause. These will include all datasets to join with.
            buildStartingFromClause(sb);

            // Add the UNNEST steps expressions. These will always follow after the FROM expressions.
            buildUnnestSteps(sb);

            // Add the JOIN steps expressions. These will always follow after the UNNEST step expressions.
            buildJoinSteps(sb);

            // Add the WHERE clauses next. This will include implicit JOINs and quantified expressions.
            buildWhereClause(sb);

            // Add the SELECT clause. This consists of the primary keys from all involved datasets.
            buildSelectClause(sb);

            // We will ORDER BY all fields in our SELECT clause, and LIMIT accordingly.
            buildQuerySuffix(sb);

            arrayQuery.queryString = sb.toString();
            return arrayQuery;
        }

        private void buildStartingFromClause(StringBuilder sb) {
            for (int i = 0; i < numberOfJoins; i++) {
                FromExpr fromExpr = new FromExpr("ProbeDataset" + (i + 1), "D" + (i + 2));
                arrayQuery.fromExprs.add(fromExpr);
            }
            if (numberOfJoins == 0 || numberOfJoins != numberOfExplicitJoins) {
                arrayQuery.fromExprs.add(new FromExpr(arrayIndex.getDatasetName(), "D1"));
            }

            sb.append("FROM ");
            sb.append(arrayQuery.fromExprs.stream().map(FromExpr::toString).collect(Collectors.joining(",\n\t")));
            sb.append('\n');
        }

        private void buildUnnestSteps(StringBuilder sb) {
            if (unnestStepDepth == 0) {
                workingUnnestAlias = "D1";

            } else {
                arrayQuery.unnestSteps.addAll(ArrayQueryUtil.createUnnestSteps(valueSupplier, "D1",
                        arrayIndex.getArrayPath(), unnestStepDepth, "P", "G", "G"));
                workingUnnestAlias = arrayQuery.unnestSteps.get(arrayQuery.unnestSteps.size() - 1).alias;
                String unnestSteps =
                        arrayQuery.unnestSteps.stream().map(UnnestStep::toString).collect(Collectors.joining("\n"));
                sb.append(unnestSteps).append('\n');
            }
        }

        private void buildJoinSteps(StringBuilder sb) {
            if (numberOfExplicitJoins == 0) {
                return;
            }

            // Build the expression to JOIN with. This will be a subquery that UNNESTs the array part that has
            // not yet been UNNESTed.
            List<UnnestStep> subqueryUnnestSteps = new ArrayList<>();
            String prevSubqueryUnnestAlias = "D1";
            for (int i = unnestStepDepth; i < arrayIndex.getArrayPath().size(); i++) {
                String arrayField = String.join(".", arrayIndex.getArrayPath().get(i));
                subqueryUnnestSteps.add(new UnnestStep(prevSubqueryUnnestAlias, arrayField, "G" + (i + 1)));
                prevSubqueryUnnestAlias = "G" + (i + 1);
            }
            List<SelectExpr> subquerySelectExprs = new ArrayList<>();
            String finalSubqueryUnnestAlias = prevSubqueryUnnestAlias;
            ArrayQueryUtil.createFieldStream(arrayIndex.getElements()).forEach(e -> {
                ArrayElement.TableField tableField = e.getRight();
                String exprInSelect;
                switch (e.getLeft()) {
                    case ATOMIC:
                        exprInSelect = "D1." + tableField.getFullFieldName();
                        subquerySelectExprs.add(new SelectExpr(exprInSelect, tableField.getLastFieldName()));
                        break;
                    case UNNEST_VALUE:
                        exprInSelect = finalSubqueryUnnestAlias;
                        subquerySelectExprs.add(new SelectExpr(exprInSelect, finalSubqueryUnnestAlias));
                        break;
                    case UNNEST_OBJECT:
                        exprInSelect = finalSubqueryUnnestAlias + "." + tableField.getFullFieldName();
                        subquerySelectExprs.add(new SelectExpr(exprInSelect, tableField.getLastFieldName()));
                        break;
                }
            });

            // Now build each JOIN step.
            for (int i = 0; i < numberOfExplicitJoins; i++) {
                List<SimpleConjunct> onConjuncts = ArrayQueryUtil.createOnConjuncts(valueSupplier,
                        arrayIndex.getElements(), finalSubqueryUnnestAlias, i);
                FromExpr indexedDataset = new FromExpr(arrayIndex.getDatasetName(), "D1");
                arrayQuery.joinSteps.add(new JoinStep(subqueryUnnestSteps, subquerySelectExprs, indexedDataset,
                        onConjuncts, valueSupplier.getJoinType(), "J" + (i + 1)));
            }
            sb.append(arrayQuery.joinSteps.stream().map(JoinStep::toString).collect(Collectors.joining("\n")));
            sb.append('\n');
        }

        private void buildWhereClause(StringBuilder sb) {
            boolean isConditionOnUnnestPossible = unnestStepDepth == arrayIndex.getArrayPath().size();

            // JOIN the remaining tables.
            for (int i = arrayQuery.joinSteps.size(); i < numberOfJoins; i++) {
                int joinPosition = i;
                if (isConditionOnUnnestPossible && valueSupplier.getIsConditionOnUnnest()) {
                    // Having UNNESTed the indexed array(s), join with said array.
                    arrayQuery.whereConjuncts
                            .addAll(ArrayQueryUtil.createFieldStream(arrayIndex.getElements())
                                    .map(e -> ArrayQueryUtil.createConjunctForUnnestJoins(valueSupplier,
                                            workingUnnestAlias, e.getRight(), e.getLeft(), joinPosition))
                                    .collect(Collectors.toList()));

                } else {
                    // We are going to perform a quantified join. Join the atomic fields outside the quantification.
                    arrayQuery.whereConjuncts
                            .addAll(ArrayQueryUtil.createFieldStream(arrayIndex.getElements())
                                    .filter(e -> e.getLeft() == ArrayElement.Kind.ATOMIC)
                                    .map(e -> ArrayQueryUtil.createConjunctForUnnestJoins(valueSupplier,
                                            workingUnnestAlias, e.getRight(), e.getLeft(), joinPosition))
                                    .collect(Collectors.toList()));

                    // Create the quantified expression conjunct.
                    arrayQuery.whereConjuncts.add(ArrayQueryUtil.createConjunctForQuantifiedJoins(valueSupplier,
                            arrayIndex.getElements(), arrayIndex.getArrayPath(), "D1", joinPosition));
                }
            }

            // If there are no JOINs, then we must condition on all constants in the index.
            if (numberOfJoins == 0 || valueSupplier.getIsConstantConditionWithJoins()) {
                // Condition on the atomic fields. Similar to joins, do not include these in the quantification.
                arrayQuery.whereConjuncts.addAll(ArrayQueryUtil.createFieldStream(arrayIndex.getElements())
                        .filter(e -> e.getLeft() == ArrayElement.Kind.ATOMIC)
                        .map(e -> ArrayQueryUtil.createConjunctForUnnestNonJoins(valueSupplier,
                                "D1." + e.getRight().getFullFieldName(), e.getRight(), e.getLeft()))
                        .collect(Collectors.toList()));

                if (isConditionOnUnnestPossible && valueSupplier.getIsConditionOnUnnest()) {
                    // Having UNNESTed the indexed array(s), condition on the items in said array.
                    arrayQuery.whereConjuncts
                            .addAll(ArrayQueryUtil.createFieldStream(arrayIndex.getElements())
                                    .filter(e -> e.getLeft() != ArrayElement.Kind.ATOMIC)
                                    .map(e -> ArrayQueryUtil.createConjunctForUnnestNonJoins(valueSupplier,
                                            workingUnnestAlias, e.getRight(), e.getLeft()))
                                    .collect(Collectors.toList()));

                } else {
                    // Create the quantification expression conjunct.
                    arrayQuery.whereConjuncts.add(ArrayQueryUtil.createConjunctForQuantifiedNonJoins(valueSupplier,
                            arrayIndex.getElements(), arrayIndex.getArrayPath(), "D1"));
                }
            }

            // If there is more than one probe, we must join each probe.
            for (int i = 1; i < numberOfJoins; i++) {
                String primaryKeyOne = "D" + (i + 1) + "._id";
                String primaryKeyTwo = "D" + (i + 2) + "._id";
                if (!valueSupplier.getIsCrossProductOnProbes()) {
                    arrayQuery.whereConjuncts.add(new SimpleConjunct(primaryKeyOne, primaryKeyTwo, "=", null));
                }
            }

            if (!arrayQuery.whereConjuncts.isEmpty()) {
                String d = " AND\n\t";
                sb.append("WHERE ");
                sb.append(arrayQuery.whereConjuncts.stream().map(Conjunct::toString).collect(Collectors.joining(d)));
                sb.append('\n');
            }
        }

        private void buildSelectClause(StringBuilder sb) {
            sb.append("SELECT ");
            if (numberOfJoins == 0 || numberOfJoins != numberOfExplicitJoins) {
                arrayQuery.selectExprs.add(new SelectExpr("D1._id", "PK1"));
            }
            for (int i = 0; i < numberOfJoins; i++) {
                arrayQuery.selectExprs.add(new SelectExpr("D" + (i + 2) + "._id", "PK" + (i + 2)));
            }
            sb.append(arrayQuery.selectExprs.stream().map(SelectExpr::toString).collect(Collectors.joining(", ")));
            sb.append('\n');
        }

        private void buildQuerySuffix(StringBuilder sb) {
            sb.append("ORDER BY ");
            sb.append(arrayQuery.selectExprs.stream().map(e -> e.expr).collect(Collectors.joining(", "))).append('\n');
            sb.append("LIMIT ").append(limitCount).append(";\n");
        }
    }
}
