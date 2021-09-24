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
import java.util.Collections;
import java.util.List;

public class ArrayIndex {
    private final List<ArrayElement> elements;
    private final List<List<String>> arrayPath;
    private String ddlStatement;
    private String indexName;
    private String datasetName;

    @Override
    public String toString() {
        return ddlStatement;
    }

    public List<ArrayElement> getElements() {
        return elements;
    }

    public List<List<String>> getArrayPath() {
        return arrayPath;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    private ArrayIndex(List<ArrayElement> elements, List<List<String>> arrayPath) {
        this.elements = Collections.unmodifiableList(elements);
        this.arrayPath = Collections.unmodifiableList(arrayPath);
    }

    public static class Builder {
        public static abstract class ValueSupplier {
            public abstract BaseWisconsinTable.Field getAtomicBaseField();

            public abstract BaseWisconsinTable.Field getArrayBaseField();

            public abstract List<String> getFieldName(BaseWisconsinTable.Field baseField);

            public abstract List<String> getGroupFieldName(int nestingLevel);
        }

        private int numberOfAtomicPrefixes;
        private int numberOfFieldsInArray;
        private int numberOfAtomicSuffixes;
        private boolean isArrayOfScalars;
        private int depthOfArray;

        private final String indexName;
        private final String datasetName;
        private ValueSupplier valueSupplier;

        public void setNumberOfAtomicPrefixes(int numberOfAtomicPrefixes) {
            this.numberOfAtomicPrefixes = numberOfAtomicPrefixes;
        }

        public void setNumberOfFieldsInArray(int numberOfFieldsInArray) {
            this.numberOfFieldsInArray = numberOfFieldsInArray;
        }

        public void setNumberOfAtomicSuffixes(int numberOfAtomicSuffixes) {
            this.numberOfAtomicSuffixes = numberOfAtomicSuffixes;
        }

        public void setDepthOfArray(int depthOfArray) {
            this.depthOfArray = depthOfArray;
        }

        public void setIsArrayOfScalars(boolean isArrayOfScalars) {
            this.isArrayOfScalars = isArrayOfScalars;
        }

        public void setValueSupplier(ValueSupplier valueSupplier) {
            this.valueSupplier = valueSupplier;
        }

        public Builder(String indexName, String datasetName) {
            this.indexName = indexName;
            this.datasetName = datasetName;
        }

        public ArrayIndex build() {
            final List<ArrayElement> elements = new ArrayList<>();
            final List<List<String>> arrayPath = new ArrayList<>();

            for (int i = 0; i < numberOfAtomicPrefixes; i++) {
                ArrayElement element = new ArrayElement(ArrayElement.Kind.ATOMIC, elements.size());
                BaseWisconsinTable.Field field = valueSupplier.getAtomicBaseField();
                List<String> fieldName = valueSupplier.getFieldName(field);
                element.projectList.add(new ArrayElement.TableField(fieldName, field));
                elements.add(element);
            }

            ArrayElement arrayElement = new ArrayElement(
                    (isArrayOfScalars) ? ArrayElement.Kind.UNNEST_VALUE : ArrayElement.Kind.UNNEST_OBJECT,
                    elements.size());
            for (int i = 0; i < depthOfArray; i++) {
                ArrayElement.TableField tableField;
                if (isArrayOfScalars && i == depthOfArray - 1) {
                    BaseWisconsinTable.Field field = valueSupplier.getArrayBaseField();
                    List<String> fieldName = valueSupplier.getFieldName(field);
                    tableField = new ArrayElement.TableField(fieldName, field);
                } else {
                    List<String> fieldName = valueSupplier.getGroupFieldName(i + 1);
                    tableField = new ArrayElement.TableField(fieldName, null);
                }
                arrayElement.unnestList.add(tableField);
                arrayPath.add(tableField.getFieldName());
            }
            if (!isArrayOfScalars) {
                for (int i = 0; i < numberOfFieldsInArray; i++) {
                    BaseWisconsinTable.Field field = valueSupplier.getArrayBaseField();
                    List<String> fieldName = valueSupplier.getFieldName(field);
                    arrayElement.projectList.add(new ArrayElement.TableField(fieldName, field));
                }
            }
            elements.add(arrayElement);

            for (int i = 0; i < numberOfAtomicSuffixes; i++) {
                ArrayElement element = new ArrayElement(ArrayElement.Kind.ATOMIC, elements.size());
                BaseWisconsinTable.Field field = valueSupplier.getAtomicBaseField();
                List<String> fieldName = valueSupplier.getFieldName(field);
                element.projectList.add(new ArrayElement.TableField(fieldName, field));
                elements.add(element);
            }

            ArrayIndex index = new ArrayIndex(elements, arrayPath);
            index.ddlStatement = buildIndexDDL(index.elements);
            index.datasetName = datasetName;
            index.indexName = indexName;
            return index;
        }

        private String buildIndexDDL(List<ArrayElement> elements) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE INDEX ").append(indexName);
            sb.append(" ON ").append(datasetName).append(" ( ");
            for (int i = 0; i < elements.size(); i++) {
                ArrayElement e = elements.get(i);
                if (!e.unnestList.isEmpty()) {
                    sb.append("( ");
                }
                for (ArrayElement.TableField unnestPart : e.unnestList) {
                    sb.append("UNNEST ");
                    sb.append(unnestPart.getFullFieldName());
                    sb.append(" ");
                }
                if (e.projectList.isEmpty()) {
                    sb.append(": ").append(e.unnestList.get(e.unnestList.size() - 1).getFieldType()).append(" )");
                } else if (!e.unnestList.isEmpty()) {
                    sb.append("SELECT ");
                    List<ArrayElement.TableField> projectList = e.projectList;
                    for (int j = 0; j < projectList.size(); j++) {
                        sb.append(projectList.get(j).getFullFieldName());
                        sb.append(": ").append(projectList.get(j).getFieldType());
                        sb.append((j != projectList.size() - 1) ? ", " : " ");
                    }
                    sb.append(" )");
                } else {
                    sb.append(e.projectList.get(0).getFullFieldName());
                    sb.append(": ").append(e.projectList.get(0).getFieldType());
                }
                if (i < elements.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append(" ) EXCLUDE UNKNOWN KEY;\n");
            return sb.toString();
        }
    }
}
