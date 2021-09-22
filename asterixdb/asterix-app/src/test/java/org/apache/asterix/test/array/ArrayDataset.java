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

import static org.apache.asterix.test.array.ArrayElement.TableField;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.IntNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.TextNode;

public class ArrayDataset {
    public static final String[] ADDITIONAL_GROUPS = new String[] { "extra_grouping_5_a", "extra_grouping_5_b" };
    public static final String[] ADDITIONAL_FIELDS = new String[] { "extra_integer_1_a", "extra_integer_1_b" };
    public static final String[] VALUES_FOR_ADDITIONAL_GROUPS = new String[] { "1", "2", "3", "4", "5" };
    public static final String[] VALUES_FOR_ADDITIONAL_FIELDS = new String[] { "1", "2" };

    public static final String[] CONTAINED_OBJECT_NAMES = new String[] { "contained_object_1", "contained_object_2" };
    public static final String[] CONTAINER_OBJECT_NAMES = new String[] { "container_object_A", "container_object_B",
            "container_object_C", "container_object_D", "container_object_E" };

    private String fromBaseQuery;

    @Override
    public String toString() {
        return fromBaseQuery;
    }

    public static class Builder {
        private final JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(false);
        private final List<ArrayElement> builderElements = new ArrayList<>();

        public void addElement(ArrayElement element) {
            builderElements.add(element);
        }

        public ArrayDataset build() {
            ArrayElement e = builderElements.stream().filter(m -> !m.unnestList.isEmpty()).findFirst().orElseThrow();
            List<TableField> f = builderElements.stream().filter(m -> m.unnestList.isEmpty())
                    .map(m -> m.projectList.get(0)).collect(Collectors.toUnmodifiableList());
            int arrayNestingLevel = e.unnestList.size();

            // First, build the innermost array component (i.e. the SELECT clause of the DDL, or lack thereof).
            StringBuilder sb = new StringBuilder();
            sb.append("WITH group1 AS (\n");
            sb.append("\tFROM ").append(BaseWisconsinTable.TABLE_NAME).append(" D\n\t");
            appendGroupByClause(sb, e.unnestList.size());
            sb.append("\tGROUP AS G1\n");
            appendSelectClauseWithGroupTerm(sb, "G1", arrayNestingLevel, e.unnestList.get(arrayNestingLevel - 1),
                    buildStartingSelectValue(e));
            sb.append(")\n");

            // If we have any nested arrays, additional groups.
            for (int i = 1; i < e.unnestList.size(); i++) {
                String joinFieldName = BaseWisconsinTable.GROUPING_FIELDS[arrayNestingLevel - 1].fieldName;
                sb.append(", group").append(i + 1).append(" AS (\n");
                sb.append("\tFROM ").append(BaseWisconsinTable.TABLE_NAME).append(" D\n\t");
                sb.append("JOIN group").append(i).append(" G").append(i).append("\n\tON ");
                sb.append("D.").append(joinFieldName);
                sb.append(" = G").append(i).append('.').append(joinFieldName).append("\n\t");
                appendGroupByClause(sb, arrayNestingLevel - 1);
                sb.append("\tGROUP AS G").append(i + 1).append('\n');
                appendSelectClauseWithGroupTerm(sb, "G" + (i + 1), arrayNestingLevel - 1,
                        e.unnestList.get(arrayNestingLevel - 2), String.format("%s_inner.G%d", "G" + (i + 1), i));
                sb.append(")\n");
                arrayNestingLevel--;
            }

            // Add the final SELECT clause.
            String joinFieldName = BaseWisconsinTable.GROUPING_FIELDS[arrayNestingLevel - 1].fieldName;
            sb.append("FROM ").append(BaseWisconsinTable.TABLE_NAME).append(" D\n");
            sb.append("JOIN group").append(e.unnestList.size()).append(" G").append(e.unnestList.size());
            sb.append("\nON D.").append(joinFieldName).append(" = G");
            sb.append(e.unnestList.size()).append('.').append(joinFieldName).append("\n");
            appendSelectClauseWithoutGroupTerm(sb, "G" + e.unnestList.size(), arrayNestingLevel - 1,
                    e.unnestList.get(0), f);

            // Return the new array dataset.
            ArrayDataset arrayDataset = new ArrayDataset();
            arrayDataset.fromBaseQuery = sb.toString();
            return arrayDataset;
        }

        private void appendGroupByClause(StringBuilder sb, int numGroupFields) {
            sb.append("GROUP BY D.").append(BaseWisconsinTable.GROUPING_FIELDS[0].fieldName).append(" ");
            for (int i = 1; i < numGroupFields; i++) {
                sb.append(", D.").append(BaseWisconsinTable.GROUPING_FIELDS[i].fieldName).append(' ');
            }
            sb.append('\n');
        }

        private void appendSelectClauseWithGroupTerm(StringBuilder sb, String groupAlias, int numGroupFields,
                TableField groupField, String groupValue) {
            ObjectNode selectClauseNode = new ObjectNode(jsonNodeFactory);

            // Append the GROUP BY fields.
            for (int i = 0; i < numGroupFields; i++) {
                selectClauseNode.put(BaseWisconsinTable.GROUPING_FIELDS[i].fieldName,
                        String.format("$D.%s$", BaseWisconsinTable.GROUPING_FIELDS[i].fieldName));
            }

            // Append two extra groups alongside the group-to-be-added.
            List<JsonNode> additionalGroupValues = Stream.of(VALUES_FOR_ADDITIONAL_GROUPS)
                    .map(v -> new IntNode(Integer.parseInt(v))).collect(Collectors.toList());
            for (String additionalGroup : ADDITIONAL_GROUPS) {
                selectClauseNode.set(additionalGroup, new ArrayNode(jsonNodeFactory, additionalGroupValues));
            }

            // Create the group field.
            String q = String.format("$( FROM %s %<s_inner SELECT VALUE %s )$", groupAlias, groupValue);
            appendNestedFieldsToObjectNode(groupField, selectClauseNode, new TextNode(q));

            // Serialize our object into a SELECT clause.
            sb.append("\tSELECT VALUE ").append(buildJSONForQuery(selectClauseNode));
        }

        private void appendSelectClauseWithoutGroupTerm(StringBuilder sb, String groupAlias, int numGroupFields,
                TableField groupField, List<TableField> auxiliaryFields) {
            ObjectNode selectClauseNode = new ObjectNode(jsonNodeFactory);

            // Append the GROUP BY fields.
            for (int i = 0; i < numGroupFields; i++) {
                selectClauseNode.put(BaseWisconsinTable.GROUPING_FIELDS[i].fieldName,
                        String.format("$D.%s$", BaseWisconsinTable.GROUPING_FIELDS[i].fieldName));
            }

            // Append two extra groups alongside the group-to-be-added.
            List<JsonNode> additionalGroupValues = Stream.of(VALUES_FOR_ADDITIONAL_GROUPS)
                    .map(v -> new IntNode(Integer.parseInt(v))).collect(Collectors.toList());
            for (String additionalGroup : ADDITIONAL_GROUPS) {
                selectClauseNode.set(additionalGroup, new ArrayNode(jsonNodeFactory, additionalGroupValues));
            }

            // Add / create our auxiliary objects.
            for (TableField field : auxiliaryFields) {
                String v = buildTableFieldValue("D." + field.getSourceField().fieldName, field);
                appendNestedFieldsToObjectNode(field, selectClauseNode, new TextNode(String.format("$%s$", v)));
            }

            // Finally, add our group field. This array should already be formed.
            String q = String.format("$%s.%s$", groupAlias, groupField.getFullFieldName());
            appendNestedFieldsToObjectNode(groupField, selectClauseNode, new TextNode(q));

            // Serialize our object into a SELECT clause.
            sb.append("SELECT VALUE ").append(buildJSONForQuery(selectClauseNode));
        }

        private String buildStartingSelectValue(ArrayElement element) {
            StringBuilder sb = new StringBuilder();
            if (element.projectList.isEmpty()) {
                TableField workingField = element.unnestList.get(element.unnestList.size() - 1);
                String fieldName = "G1_inner.D." + workingField.getSourceField().fieldName;
                sb.append(buildTableFieldValue(fieldName, workingField));

            } else {
                ObjectNode selectValueNode = new ObjectNode(jsonNodeFactory);

                // Append extra fields within the object item. These values will always be fixed.
                for (int i = 0; i < ADDITIONAL_FIELDS.length; i++) {
                    int additionalFieldValue = Integer.parseInt(VALUES_FOR_ADDITIONAL_FIELDS[i]);
                    selectValueNode.set(ADDITIONAL_FIELDS[i], new IntNode(additionalFieldValue));
                }

                // Append the items within our object itself.
                for (TableField workingField : element.projectList) {
                    String fieldName = "G1_inner.D." + workingField.getSourceField().fieldName;
                    String v = buildTableFieldValue(fieldName, workingField);
                    appendNestedFieldsToObjectNode(workingField, selectValueNode,
                            new TextNode(String.format("$%s$", v)));
                }

                // Serialize our value node.
                sb.append(buildJSONForQuery(selectValueNode));
            }

            return sb.toString();
        }

        private void appendNestedFieldsToObjectNode(TableField field, ObjectNode objectNode, TextNode endpointValue) {
            ObjectNode workingObject = objectNode;
            for (int i = 0; i < field.getFieldName().size(); i++) {
                String fieldPart = field.getFieldName().get(i);
                if (i < field.getFieldName().size() - 1) {
                    if (workingObject.get(fieldPart) == null) {
                        ObjectNode objectInsideWorkingObject = new ObjectNode(jsonNodeFactory);
                        workingObject.set(fieldPart, objectInsideWorkingObject);
                        workingObject = objectInsideWorkingObject;

                    } else {
                        workingObject = (ObjectNode) workingObject.get(fieldPart);
                    }

                } else {
                    workingObject.set(fieldPart, endpointValue);
                }
            }
        }

        private String buildTableFieldValue(String fieldName, TableField field) {
            switch (field.getFieldType()) {
                case BIGINT:
                    return fieldName;
                case DOUBLE:
                    return String.format("(%s + 0.5)", fieldName.replace("double", "integer"));
                case STRING:
                    return String.format("CODEPOINT_TO_STRING([100 + %s])", fieldName);
            }
            throw new UnsupportedOperationException("Unsupported type for field: " + field.getFieldType().toString());
        }

        private String buildJSONForQuery(ObjectNode node) {
            return node.toString().replace("\"$", "").replace("$\"", "").replace("\\\"", "\"");
        }
    }
}
