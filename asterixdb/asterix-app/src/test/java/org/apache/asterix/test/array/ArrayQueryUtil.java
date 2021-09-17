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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.test.array.ArrayElement.Kind;
import org.apache.asterix.test.array.ArrayElement.TableField;
import org.apache.asterix.test.array.ArrayQuery.Builder.ValueSupplier;
import org.apache.asterix.test.array.ArrayQuery.Conjunct;
import org.apache.asterix.test.array.ArrayQuery.ExistsConjunct;
import org.apache.asterix.test.array.ArrayQuery.FromExpr;
import org.apache.asterix.test.array.ArrayQuery.QuantifiedConjunct;
import org.apache.asterix.test.array.ArrayQuery.SimpleConjunct;
import org.apache.asterix.test.array.ArrayQuery.UnnestStep;
import org.apache.commons.lang3.tuple.Pair;

class ArrayQueryUtil {
    public static Stream<Pair<Kind, TableField>> createFieldStream(List<ArrayElement> elements) {
        return elements.stream().map(e -> {
            switch (e.kind) {
                case ATOMIC:
                    TableField atomicField = e.projectList.get(0);
                    return Collections.singletonList(Pair.of(e.kind, atomicField));

                case UNNEST_VALUE:
                    TableField lastUnnestField = e.unnestList.get(e.unnestList.size() - 1);
                    return Collections.singletonList(Pair.of(e.kind, lastUnnestField));

                default: // UNNEST_OBJECT
                    return e.projectList.stream().map(p -> Pair.of(e.kind, p)).collect(Collectors.toUnmodifiableList());
            }
        }).flatMap(Collection::stream);
    }

    public static List<UnnestStep> createUnnestSteps(ValueSupplier valueSupplier, String startingAlias,
            List<List<String>> arrayPath, int depthToUnnest, String prefixA, String prefixB, String prefixC) {
        List<UnnestStep> unnestSteps = new ArrayList<>();
        String workingAlias = startingAlias;
        for (int i = 0; i < depthToUnnest; i++) {
            List<UnnestStep> extraUnnestSteps = valueSupplier.getExtraUnnestSteps(workingAlias);
            for (int j = 0; j < extraUnnestSteps.size(); j++) {
                String aliasForExtraUnnest = prefixA.repeat(i + 1) + (j + 1);
                UnnestStep extraUnnestStep = extraUnnestSteps.get(j);
                UnnestStep extraUnnestStepWithAlias =
                        new UnnestStep(extraUnnestStep.sourceAlias, extraUnnestStep.arrayField, aliasForExtraUnnest);
                unnestSteps.add(extraUnnestStepWithAlias);
            }

            String arrayField = String.join(".", arrayPath.get(i));
            UnnestStep unnestStep = new UnnestStep(workingAlias, arrayField, prefixB + (i + 1));
            unnestSteps.add(unnestStep);
            workingAlias = prefixC + (i + 1);
        }
        return unnestSteps;
    }

    public static List<SimpleConjunct> createOnConjuncts(ValueSupplier valueSupplier,
            List<ArrayElement> arrayIndexElements, String finalSubqueryUnnestAlias, int joinPosition) {
        return ArrayQueryUtil.createFieldStream(arrayIndexElements).map(e -> {
            TableField tableField = e.getRight();
            String leftExpr = null;
            switch (e.getLeft()) {
                case ATOMIC:
                case UNNEST_OBJECT:
                    leftExpr = "J" + (joinPosition + 1) + "." + tableField.getLastFieldName();
                    break;
                case UNNEST_VALUE:
                    leftExpr = "J" + (joinPosition + 1) + "." + finalSubqueryUnnestAlias;
                    break;
            }
            String rightExpr = createProbeExpr(joinPosition, tableField);
            String operator = valueSupplier.getOperatorForJoin();
            return new SimpleConjunct(leftExpr, rightExpr, operator, "/* +indexnl */");
        }).collect(Collectors.toList());
    }

    public static SimpleConjunct createConjunctForUnnestJoins(ValueSupplier valueSupplier, String unnestAlias,
            ArrayElement.TableField tableField, ArrayElement.Kind fieldKind, int joinPosition) {
        String leftExpr = null;
        switch (fieldKind) {
            case ATOMIC:
                leftExpr = "D1." + tableField.getFullFieldName();
                break;
            case UNNEST_VALUE:
                leftExpr = unnestAlias;
                break;
            case UNNEST_OBJECT:
                leftExpr = unnestAlias + "." + tableField.getFullFieldName();
                break;
        }
        String rightExpr = createProbeExpr(joinPosition, tableField);
        return new SimpleConjunct(leftExpr, rightExpr, valueSupplier.getOperatorForJoin(), "/* +indexnl */");
    }

    public static QuantifiedConjunct createConjunctForQuantifiedJoins(ValueSupplier valueSupplier,
            List<ArrayElement> arrayIndexElements, List<List<String>> workingArray, String prefix, int joinPosition) {
        List<String> arraysToQuantify = new ArrayList<>();
        List<String> quantifiedVars = new ArrayList<>();
        List<Conjunct> conjuncts = new ArrayList<>();
        int remainingDepthOfArray = workingArray.size();

        if (remainingDepthOfArray > 1) {
            String containingArray = String.join(".", workingArray.get(0));
            arraysToQuantify.add(String.format("%s.%s", prefix, containingArray));
            String itemVar = "V" + remainingDepthOfArray;
            quantifiedVars.add(itemVar);

            if (valueSupplier.getIsUseNestedQuantification()) {
                // Recurse and create a nested quantification expression to quantify over the rest of our arrays.
                conjuncts.add(createConjunctForQuantifiedJoins(valueSupplier, arrayIndexElements,
                        workingArray.subList(1, workingArray.size()), itemVar, joinPosition));

            } else {
                // We are now using an EXISTS clause to quantify over the rest of our arrays.
                List<List<String>> remainingArray = workingArray.subList(1, workingArray.size());
                conjuncts.add(createConjunctForExistsJoins(valueSupplier, arrayIndexElements, remainingArray, itemVar,
                        joinPosition));
            }

        } else {
            conjuncts.addAll(createFieldStream(arrayIndexElements).filter(e -> e.getLeft() != Kind.ATOMIC)
                    .peek(e -> appendQuantifiedVars(workingArray, prefix, arraysToQuantify, quantifiedVars, e))
                    .map(e -> {
                        TableField tableField = e.getRight();
                        String operator = valueSupplier.getOperatorForJoin();
                        String leftExpr = null;
                        switch (e.getLeft()) {
                            case ATOMIC:
                                throw new IllegalStateException("Unexpected atomic element!");
                            case UNNEST_VALUE:
                                leftExpr = "V1";
                                break;
                            case UNNEST_OBJECT:
                                leftExpr = "V1." + tableField.getFullFieldName();
                                break;
                        }
                        String rightExpr = createProbeExpr(joinPosition, tableField);
                        return new SimpleConjunct(leftExpr, rightExpr, operator, "/* +indexnl */");
                    }).collect(Collectors.toList()));
        }

        return new QuantifiedConjunct(arraysToQuantify, quantifiedVars, conjuncts, valueSupplier.getQuantifier());
    }

    public static SimpleConjunct createConjunctForUnnestNonJoins(ValueSupplier valueSupplier, String unnestAlias,
            ArrayElement.TableField tableField, ArrayElement.Kind fieldKind) {
        String variableExpr = null;
        switch (fieldKind) {
            case ATOMIC:
                variableExpr = "D1." + tableField.getFullFieldName();
                break;
            case UNNEST_VALUE:
                variableExpr = unnestAlias;
                break;
            case UNNEST_OBJECT:
                variableExpr = unnestAlias + "." + tableField.getFullFieldName();
                break;
        }

        return createSimpleConjunctForNonJoins(valueSupplier, tableField, variableExpr);
    }

    public static QuantifiedConjunct createConjunctForQuantifiedNonJoins(ValueSupplier valueSupplier,
            List<ArrayElement> indexElements, List<List<String>> workingArray, String prefix) {
        List<String> arraysToQuantify = new ArrayList<>();
        List<String> quantifiedVars = new ArrayList<>();
        List<Conjunct> conjuncts = new ArrayList<>();
        int remainingDepthOfArray = workingArray.size();

        if (remainingDepthOfArray > 1) {
            String containingArray = String.join(".", workingArray.get(0));
            arraysToQuantify.add(String.format("%s.%s", prefix, containingArray));
            String itemVar = "V" + remainingDepthOfArray;
            quantifiedVars.add(itemVar);

            if (valueSupplier.getIsUseNestedQuantification()) {
                // Recurse and create a nested quantification expression to quantify over the rest of our arrays.
                conjuncts.add(createConjunctForQuantifiedNonJoins(valueSupplier, indexElements,
                        workingArray.subList(1, workingArray.size()), itemVar));

            } else {
                // We are now using an EXISTS clause to quantify over the rest of our arrays.
                List<List<String>> remainingArray = workingArray.subList(1, workingArray.size());
                conjuncts.add(createConjunctForExistsNonJoins(valueSupplier, indexElements, remainingArray, itemVar));
            }

        } else {
            conjuncts.addAll(createFieldStream(indexElements).filter(e -> e.getLeft() != Kind.ATOMIC)
                    .peek(e -> appendQuantifiedVars(workingArray, prefix, arraysToQuantify, quantifiedVars, e))
                    .map(e -> {
                        ArrayElement.TableField tableField = e.getRight();
                        String variableExpr = null;
                        switch (e.getLeft()) {
                            case ATOMIC:
                                throw new IllegalStateException("Unexpected atomic element!");
                            case UNNEST_VALUE:
                                variableExpr = "V1";
                                break;
                            case UNNEST_OBJECT:
                                variableExpr = "V1." + tableField.getFullFieldName();
                                break;
                        }
                        return createSimpleConjunctForNonJoins(valueSupplier, tableField, variableExpr);
                    }).collect(Collectors.toList()));
        }

        // Append any extra conjuncts from the indexed object.
        if (ArrayQueryUtil.createFieldStream(indexElements).anyMatch(e -> e.getLeft() == Kind.UNNEST_OBJECT)) {
            conjuncts.addAll(valueSupplier.getExtraConjuncts("V" + remainingDepthOfArray));
        }

        // Add any extra quantifiers.
        conjuncts.addAll(valueSupplier.getExtraQuantifiersAndConjuncts(arraysToQuantify, quantifiedVars, prefix));

        return new QuantifiedConjunct(arraysToQuantify, quantifiedVars, conjuncts, valueSupplier.getQuantifier());
    }

    private static void appendQuantifiedVars(List<List<String>> workingArray, String prefix,
            List<String> arraysToQuantify, List<String> quantifiedVars, Pair<Kind, TableField> e) {
        TableField tableField = e.getRight();
        switch (e.getLeft()) {
            case ATOMIC:
                throw new IllegalStateException("Unexpected atomic element!");
            case UNNEST_VALUE:
                arraysToQuantify.add(String.format("%s.%s", prefix, tableField.getFullFieldName()));
                quantifiedVars.add("V1");
                break;
            case UNNEST_OBJECT:
                if (!quantifiedVars.contains("V1")) {
                    String containingArray = String.join(".", workingArray.get(0));
                    arraysToQuantify.add(String.format("%s.%s", prefix, containingArray));
                    quantifiedVars.add("V1");
                }
                break;
        }
    }

    private static ExistsConjunct createConjunctForExistsJoins(ValueSupplier valueSupplier,
            List<ArrayElement> arrayIndexElements, List<List<String>> arrayPath, String itemVar, int joinPosition) {
        // Build our FROM and UNNEST clauses.
        List<UnnestStep> unnestSteps = new ArrayList<>();
        if (arrayPath.size() > 1) {
            String prefixA = Character.toString('T' + joinPosition);
            String prefixB = Character.toString('O' + joinPosition);
            unnestSteps.addAll(ArrayQueryUtil.createUnnestSteps(valueSupplier, "GG1",
                    arrayPath.subList(1, arrayPath.size()), arrayPath.size() - 1, prefixA, prefixB, prefixB));
        }
        FromExpr fromExpr = new FromExpr(String.format("%s.%s", itemVar, String.join(".", arrayPath.get(0))), "GG1");

        // Create the conjuncts for the JOIN.
        String variableExpr = (unnestSteps.isEmpty()) ? "GG1" : unnestSteps.get(unnestSteps.size() - 1).alias;
        List<SimpleConjunct> conjuncts =
                createFieldStream(arrayIndexElements).filter(e -> e.getLeft() != Kind.ATOMIC).map(e -> {
                    TableField tableField = e.getRight();
                    String fieldName = tableField.getFullFieldName();
                    String operator = valueSupplier.getOperatorForJoin();
                    String leftExpr = null;
                    switch (e.getLeft()) {
                        case ATOMIC:
                            throw new IllegalStateException("Unexpected atomic element!");
                        case UNNEST_VALUE:
                            leftExpr = variableExpr;
                            break;
                        case UNNEST_OBJECT:
                            leftExpr = variableExpr + "." + fieldName;
                            break;
                    }
                    String rightExpr = createProbeExpr(joinPosition, tableField);
                    return new SimpleConjunct(leftExpr, rightExpr, operator, "/* +indexnl */");
                }).collect(Collectors.toList());

        return new ExistsConjunct(fromExpr, unnestSteps, conjuncts);
    }

    private static String createProbeExpr(int joinPosition, TableField tableField) {
        String rightExpr = "D" + (joinPosition + 2) + "." + tableField.getSourceField().fieldName;
        switch (tableField.getFieldType()) {
            case BIGINT:
                rightExpr = String.format("TO_BIGINT(%s)", rightExpr);
                break;
            case DOUBLE:
                rightExpr = String.format("(TO_DOUBLE(%s) + 0.5)", rightExpr.replace("double", "integer"));
                break;
            case STRING:
                rightExpr = String.format("CODEPOINT_TO_STRING([100 + %s])", rightExpr);
                break;
        }
        return rightExpr;
    }

    private static SimpleConjunct createSimpleConjunctForNonJoins(ValueSupplier valueSupplier, TableField tableField,
            String variableExpr) {
        Pair<String, String> bounds = valueSupplier.getRangeFromDomain(tableField.getSourceField().domain);
        if (valueSupplier.getIsBetweenConjunct()) {
            return new SimpleConjunct(variableExpr, bounds.getLeft(), bounds.getRight(), "BETWEEN", null);

        } else {
            switch (valueSupplier.getOperatorForConstant()) {
                case "=":
                    return new SimpleConjunct(variableExpr, bounds.getLeft(), "=", null);
                case "<":
                    return new SimpleConjunct(variableExpr, bounds.getRight(), "<", null);
                case ">":
                    return new SimpleConjunct(variableExpr, bounds.getLeft(), ">", null);
                case "<=":
                    return new SimpleConjunct(variableExpr, bounds.getRight(), "<=", null);
                default: // ">="
                    return new SimpleConjunct(variableExpr, bounds.getLeft(), ">=", null);
            }
        }
    }

    private static ExistsConjunct createConjunctForExistsNonJoins(ValueSupplier valueSupplier,
            List<ArrayElement> arrayIndexElements, List<List<String>> arrayPath, String itemVar) {
        // Build our FROM and UNNEST clauses.
        List<UnnestStep> unnestSteps = new ArrayList<>();
        if (arrayPath.size() > 1) {
            unnestSteps.addAll(ArrayQueryUtil.createUnnestSteps(valueSupplier, "GG1",
                    arrayPath.subList(1, arrayPath.size()), arrayPath.size() - 1, "S", "N", "N"));
        }
        FromExpr fromExpr = new FromExpr(String.format("%s.%s", itemVar, String.join(".", arrayPath.get(0))), "GG1");

        // Create the conjuncts for the JOIN.
        String variableExpr = (unnestSteps.isEmpty()) ? "GG1" : unnestSteps.get(unnestSteps.size() - 1).alias;
        List<SimpleConjunct> conjuncts =
                createFieldStream(arrayIndexElements).filter(e -> e.getLeft() != Kind.ATOMIC).map(e -> {
                    TableField tableField = e.getRight();
                    String variableExprForConjunct = null;
                    switch (e.getLeft()) {
                        case ATOMIC:
                            throw new IllegalStateException("Unexpected atomic element!");
                        case UNNEST_VALUE:
                            variableExprForConjunct = variableExpr;
                            break;
                        case UNNEST_OBJECT:
                            variableExprForConjunct = variableExpr + "." + tableField.getFullFieldName();
                            break;
                    }
                    return createSimpleConjunctForNonJoins(valueSupplier, tableField, variableExprForConjunct);
                }).collect(Collectors.toList());

        return new ExistsConjunct(fromExpr, unnestSteps, conjuncts);
    }
}
