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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.test.array.ArrayQuery.Builder.ValueSupplier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.random.RandomGenerator;

public class ValueSupplierFactory {
    private final ExponentialDistribution distBelow1;
    private final ExponentialDistribution distEqual1;
    private final ExponentialDistribution distAbove1;
    private final RandomGenerator randomGenerator;

    public ValueSupplierFactory(ExponentialDistribution distBelow1, ExponentialDistribution distEqual1,
            ExponentialDistribution distAbove1, RandomGenerator randomGenerator) {
        this.distBelow1 = distBelow1;
        this.distEqual1 = distEqual1;
        this.distAbove1 = distAbove1;
        this.randomGenerator = randomGenerator;
    }

    private class BaseArrayIndexValueSupplier extends ArrayIndex.Builder.ValueSupplier {
        private final Set<BaseWisconsinTable.Field> consumedFields = new LinkedHashSet<>();
        private final Set<String> consumedContainedNames = new LinkedHashSet<>();

        @Override
        public BaseWisconsinTable.Field getAtomicBaseField() {
            BaseWisconsinTable.Field field;
            do {
                int index = BaseWisconsinTable.NUMBER_OF_GROUPING_FIELDS
                        + randomGenerator.nextInt(BaseWisconsinTable.NUMBER_OF_NON_GROUPING_FIELDS);
                field = BaseWisconsinTable.Field.values()[index];
            } while (consumedFields.contains(field) || field.fieldType != BaseWisconsinTable.Field.Type.BIGINT);
            consumedFields.add(field);
            return field;
        }

        @Override
        public BaseWisconsinTable.Field getArrayBaseField() {
            BaseWisconsinTable.Field field;
            do {
                int index = BaseWisconsinTable.NUMBER_OF_GROUPING_FIELDS
                        + randomGenerator.nextInt(BaseWisconsinTable.NUMBER_OF_NON_GROUPING_FIELDS);
                field = BaseWisconsinTable.Field.values()[index];
            } while (consumedFields.contains(field));
            consumedFields.add(field);
            return field;
        }

        @Override
        public List<String> getFieldName(BaseWisconsinTable.Field baseField) {
            List<String> fieldName = new ArrayList<>();
            if (randomGenerator.nextBoolean()) {
                int index = randomGenerator.nextInt(ArrayDataset.CONTAINER_OBJECT_NAMES.length);
                fieldName.add(ArrayDataset.CONTAINER_OBJECT_NAMES[index]);
            }
            fieldName.add(baseField.fieldName);
            if (randomGenerator.nextBoolean()) {
                // Three tries to generate a unique contained name. Otherwise, we default to no contained name.
                for (int i = 0; i < 3; i++) {
                    int index = randomGenerator.nextInt(ArrayDataset.CONTAINED_OBJECT_NAMES.length);
                    String containedName = ArrayDataset.CONTAINED_OBJECT_NAMES[index];
                    if (!consumedContainedNames.contains(containedName)) {
                        fieldName.add(containedName);
                        consumedContainedNames.add(containedName);
                        break;
                    }
                }
            }
            return fieldName;
        }

        @Override
        public List<String> getGroupFieldName(int nestingLevel) {
            List<String> fieldName = new ArrayList<>();
            if (randomGenerator.nextBoolean()) {
                int index = randomGenerator.nextInt(ArrayDataset.CONTAINER_OBJECT_NAMES.length);
                fieldName.add(ArrayDataset.CONTAINER_OBJECT_NAMES[index]);
            }
            fieldName.add("nesting_" + nestingLevel);
            if (randomGenerator.nextBoolean()) {
                int index = randomGenerator.nextInt(ArrayDataset.CONTAINED_OBJECT_NAMES.length);
                fieldName.add(ArrayDataset.CONTAINED_OBJECT_NAMES[index]);
            }
            return fieldName;
        }
    }

    private class BaseArrayQueryValueSupplier extends ValueSupplier {
        @Override
        public String getJoinType() {
            return ((int) Math.round(distEqual1.sample()) == 0) ? "INNER" : "LEFT OUTER";
        }

        @Override
        public String getOperatorForJoin() {
            boolean isEquiJoin = randomGenerator.nextBoolean();
            switch (isEquiJoin ? 0 : (1 + randomGenerator.nextInt(4))) {
                case 0:
                    return "=";
                case 1:
                    return "<";
                case 2:
                    return ">";
                case 3:
                    return "<=";
                default:
                    return ">=";
            }
        }

        @Override
        public String getOperatorForConstant() {
            boolean isEqualityPredicate = distBelow1.sample() > 2;
            switch (isEqualityPredicate ? 0 : (1 + randomGenerator.nextInt(4))) {
                case 0:
                    return "=";
                case 1:
                    return "<";
                case 2:
                    return ">";
                case 3:
                    return "<=";
                default:
                    return ">=";
            }
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Pair<String, String> getRangeFromDomain(BaseWisconsinTable.Domain domain) {
            if (domain.minimum instanceof Integer && domain.maximum instanceof Integer) {
                int domainMinimum = (int) domain.minimum, domainMaximum = (int) domain.maximum;
                return Pair.of(String.valueOf(domainMinimum), String.valueOf(domainMaximum));

            } else if (domain.minimum instanceof Character && domain.maximum instanceof Character) {
                int domainMinimum = (char) domain.minimum, domainMaximum = (char) domain.maximum;
                return Pair.of("\"" + (char) domainMinimum + "\"", "\"" + (char) domainMaximum + "\"");

            } else if (domain.minimum instanceof Double && domain.maximum instanceof Double) {
                int domainMinimum = (int) ((double) domain.minimum - 0.5);
                int domainMaximum = (int) ((double) domain.maximum - 0.5);
                return Pair.of(String.valueOf(domainMinimum), String.valueOf(domainMaximum));

            } else {
                throw new UnsupportedOperationException("Unknown / unsupported type for domain value.");
            }
        }

        @Override
        public boolean getIsBetweenConjunct() {
            return randomGenerator.nextBoolean();
        }

        @Override
        public String getQuantifier() {
            return (distBelow1.sample() < 1) ? "SOME" : "SOME AND EVERY";
        }

        @Override
        public boolean getIsConditionOnUnnest() {
            return randomGenerator.nextBoolean();
        }

        @Override
        public boolean getIsConstantConditionWithJoins() {
            return distBelow1.sample() > 1;
        }

        @Override
        public boolean getIsCrossProductOnProbes() {
            return distBelow1.sample() > 1;
        }

        @Override
        public boolean getIsUseNestedQuantification() {
            return randomGenerator.nextBoolean();
        }

        @Override
        public List<ArrayQuery.UnnestStep> getExtraUnnestSteps(String alias) {
            List<ArrayQuery.UnnestStep> unnestSteps = new ArrayList<>();
            int numberOfAdditionalGroups = ArrayDataset.ADDITIONAL_GROUPS.length;
            for (int i = 0; i < randomGenerator.nextInt(numberOfAdditionalGroups + 1); i++) {
                unnestSteps.add(new ArrayQuery.UnnestStep(alias, ArrayDataset.ADDITIONAL_GROUPS[i], null));
            }
            return unnestSteps;
        }

        @Override
        public List<ArrayQuery.Conjunct> getExtraConjuncts(String alias) {
            List<ArrayQuery.Conjunct> extraConjuncts = new ArrayList<>();
            if (distBelow1.sample() > 1) {
                String variableExpr = String.format("%s.%s", alias, ArrayDataset.ADDITIONAL_FIELDS[0]);
                String value = ArrayDataset.VALUES_FOR_ADDITIONAL_FIELDS[0];
                extraConjuncts.add(new ArrayQuery.SimpleConjunct(variableExpr, value, "=", null));
            }
            if (distBelow1.sample() > 1) {
                String variableExpr = String.format("%s.%s", alias, ArrayDataset.ADDITIONAL_FIELDS[1]);
                String value = ArrayDataset.VALUES_FOR_ADDITIONAL_FIELDS[1];
                extraConjuncts.add(new ArrayQuery.SimpleConjunct(variableExpr, value, "=", null));
            }
            return extraConjuncts;
        }

        @Override
        public List<ArrayQuery.Conjunct> getExtraQuantifiersAndConjuncts(List<String> arraysToQuantify,
                List<String> quantifiedVars, String alias) {
            List<ArrayQuery.Conjunct> extraConjuncts = new ArrayList<>();
            if (distBelow1.sample() > 1) {
                int i = randomGenerator.nextInt(ArrayDataset.VALUES_FOR_ADDITIONAL_GROUPS.length);
                String valueExpr = ArrayDataset.VALUES_FOR_ADDITIONAL_GROUPS[i];
                arraysToQuantify.add(String.format("%s.%s", alias, ArrayDataset.ADDITIONAL_GROUPS[0]));
                quantifiedVars.add("W1");
                extraConjuncts.add(new ArrayQuery.SimpleConjunct("W1", valueExpr, "=", null));
            }
            if (distBelow1.sample() > 1) {
                int i = randomGenerator.nextInt(ArrayDataset.VALUES_FOR_ADDITIONAL_GROUPS.length);
                String valueExpr = ArrayDataset.VALUES_FOR_ADDITIONAL_GROUPS[i];
                arraysToQuantify.add(String.format("%s.%s", alias, ArrayDataset.ADDITIONAL_GROUPS[1]));
                quantifiedVars.add("W2");
                extraConjuncts.add(new ArrayQuery.SimpleConjunct("W2", valueExpr, "=", null));
            }
            return extraConjuncts;
        }
    }

    public ArrayIndex.Builder.ValueSupplier getCompleteArrayIndexValueSupplier() {
        return new BaseArrayIndexValueSupplier();
    }

    public ValueSupplier getCompleteArrayQueryValueSupplier() {
        return new BaseArrayQueryValueSupplier();
    }

    /**
     * @return Return a supplier that generates queries that get picked up and index-accelerated by the optimizer.
     */
    public ValueSupplier getWorkingArrayQueryValueSupplier() {
        return new BaseArrayQueryValueSupplier() {
            @Override
            public boolean getIsConditionOnUnnest() {
                // TODO: This is to avoid specifying extra UNNESTs. Refer to ASTERIXDB-2962.
                return true;
            }

            @Override
            public boolean getIsConstantConditionWithJoins() {
                // TODO: This is to avoid specifying extra conjuncts in the presence of joins.
                return false;
            }

            @Override
            public boolean getIsCrossProductOnProbes() {
                // TODO: This is to avoid performing a cross product with probes. Refer to ASTERIXDB-2966.
                return false;
            }
        };
    }
}
