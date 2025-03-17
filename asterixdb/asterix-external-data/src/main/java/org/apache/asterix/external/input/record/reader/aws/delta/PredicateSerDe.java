/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.external.input.record.reader.aws.delta;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;

/**
 * Utility class to serialize and deserialize {@link Predicate} object.
 */
public class PredicateSerDe {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private PredicateSerDe() {
    }

    public static String serializeExpressionToJson(Expression expression) {
        Map<String, Object> expressionObject = visitExpression(expression);
        try {
            return OBJECT_MAPPER.writeValueAsString(expressionObject);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Optional<Predicate> deserializeExpressionFromJson(String jsonExpression) {
        try {
            if (jsonExpression == null) {
                return Optional.empty();
            }
            JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonExpression);
            return Optional.of((Predicate) visitExpression((ObjectNode) jsonNode));
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Map<String, Object> visitPredicate(Predicate predicate) {
        Map<String, Object> predicateObject = new HashMap<>();
        predicateObject.put("type", "predicate");
        predicateObject.put("name", predicate.getName());
        predicateObject.put("left", visitExpression(predicate.getChildren().get(0)));
        predicateObject.put("right", visitExpression(predicate.getChildren().get(1)));
        return predicateObject;
    }

    public static Map<String, Object> visitLiteral(Literal literal) {
        Map<String, Object> literalObject = new HashMap<>();
        literalObject.put("type", "literal");
        literalObject.put("dataType", literal.getDataType().toString());
        literalObject.put("value", literal.getValue());
        return literalObject;
    }

    public static Map<String, Object> visitColumn(Column column) {
        Map<String, Object> columnObject = new HashMap<>();
        columnObject.put("type", "column");
        columnObject.put("names", column.getNames());
        return columnObject;
    }

    private static Map<String, Object> visitExpression(Expression expression) {
        return switch (expression) {
            case Predicate predicate -> visitPredicate(predicate);
            case Column column -> visitColumn(column);
            case Literal literal -> visitLiteral(literal);
            case null, default -> throw new UnsupportedOperationException("Unsupported expression type: " + expression);
        };
    }

    public static Predicate visitPredicate(ObjectNode node) {
        return new Predicate(node.get("name").asText(), visitExpression((ObjectNode) node.get("left")),
                visitExpression((ObjectNode) node.get("right")));
    }

    public static Literal visitLiteral(ObjectNode node) {
        switch (node.get("dataType").asText()) {
            case "boolean" : return Literal.ofBoolean(node.get("value").asBoolean());
            case "byte" : return Literal.ofByte((byte) node.get("value").asInt());
            case "short" : return Literal.ofShort(node.get("value").shortValue());
            case "integer" : return Literal.ofInt(node.get("value").asInt());
            case "long" : return Literal.ofLong(node.get("value").asLong());
            case "float" : return Literal.ofFloat(node.get("value").floatValue());
            case "double" : return Literal.ofDouble(node.get("value").doubleValue());
            case "date" : return Literal.ofDate(node.get("value").asInt());
            case "timestamp" : return Literal.ofTimestamp(node.get("value").asLong());
            case "string" : return Literal.ofString(node.get("value").asText());
            case null, default : throw new UnsupportedOperationException("Unsupported literal type: " + node.get("dataType").asText());
        }
    }

    public static Column visitColumn(ObjectNode node) {
        if (node.get("names").isArray()) {
            return new Column(StreamSupport.stream(node.get("names").spliterator(), false).map(JsonNode::asText)
                    .toArray(String[]::new));
        } else {
            return new Column(node.get("names").asText());
        }
    }

    private static Expression visitExpression(ObjectNode node) {
        return switch (node.get("type").asText()) {
            case "predicate" -> visitPredicate(node);
            case "column" -> visitColumn(node);
            case "literal" -> visitLiteral(node);
            case null, default -> throw new UnsupportedOperationException("Unsupported expression type: " + node.get("type").asText());
        };
    }
}
