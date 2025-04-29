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
package org.apache.asterix.external.input.record.reader.aws.delta;

import static io.delta.kernel.internal.util.ExpressionUtils.getLeft;
import static io.delta.kernel.internal.util.ExpressionUtils.getRight;
import static io.delta.kernel.internal.util.ExpressionUtils.getUnaryChild;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static org.apache.asterix.external.input.record.reader.aws.delta.DefaultExpressionUtils.booleanWrapperVector;
import static org.apache.asterix.external.input.record.reader.aws.delta.DefaultExpressionUtils.childAt;
import static org.apache.asterix.external.input.record.reader.aws.delta.DefaultExpressionUtils.compare;
import static org.apache.asterix.external.input.record.reader.aws.delta.DefaultExpressionUtils.evalNullability;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultConstantVector;
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator;
import io.delta.kernel.expressions.AlwaysFalse;
import io.delta.kernel.expressions.AlwaysTrue;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Or;
import io.delta.kernel.expressions.PartitionValueExpression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

public class DeltaExpressionEvaluator extends DefaultExpressionEvaluator {

    private final Expression expression;

    public DeltaExpressionEvaluator(StructType structType, Expression expression, DataType dataType) {
        super(structType, expression, dataType);
        this.expression = expression;
    }

    @Override
    public ColumnVector eval(ColumnarBatch input) {
        return new ExpressionEvalVisitor(input).visit(expression);
    }

    /**
     * Implementation of {@link ExpressionVisitor} to evaluate expression on a
     * {@link ColumnarBatch}.
     */
    private static class ExpressionEvalVisitor extends ExpressionVisitor<ColumnVector> {
        private final ColumnarBatch input;

        ExpressionEvalVisitor(ColumnarBatch input) {
            this.input = input;
        }

        /*
        | Operand 1 | Operand 2 | `AND`      | `OR`       |
        |-----------|-----------|------------|------------|
        | True      | True      | True       | True       |
        | True      | False     | False      | True       |
        | True      | NULL      | NULL       | True       |
        | False     | True      | False      | True       |
        | False     | False     | False      | False      |
        | False     | NULL      | False      | NULL       |
        | NULL      | True      | NULL       | True       |
        | NULL      | False     | False      | NULL       |
        | NULL      | NULL      | NULL       | NULL       |
         */
        @Override
        ColumnVector visitAnd(And and) {
            PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(and);
            ColumnVector left = argResults.leftResult;
            ColumnVector right = argResults.rightResult;
            int numRows = argResults.rowCount;
            boolean[] result = new boolean[numRows];
            boolean[] nullability = new boolean[numRows];
            for (int rowId = 0; rowId < numRows; rowId++) {
                boolean leftIsTrue = !left.isNullAt(rowId) && left.getBoolean(rowId);
                boolean rightIsTrue = !right.isNullAt(rowId) && right.getBoolean(rowId);
                boolean leftIsFalse = !left.isNullAt(rowId) && !left.getBoolean(rowId);
                boolean rightIsFalse = !right.isNullAt(rowId) && !right.getBoolean(rowId);

                if (leftIsFalse || rightIsFalse) {
                    nullability[rowId] = false;
                    result[rowId] = false;
                } else if (leftIsTrue && rightIsTrue) {
                    nullability[rowId] = false;
                    result[rowId] = true;
                } else {
                    nullability[rowId] = true;
                    // result[rowId] is undefined when nullability[rowId] = true
                }
            }
            return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
        }

        @Override
        ColumnVector visitOr(Or or) {
            PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(or);
            ColumnVector left = argResults.leftResult;
            ColumnVector right = argResults.rightResult;
            int numRows = argResults.rowCount;
            boolean[] result = new boolean[numRows];
            boolean[] nullability = new boolean[numRows];
            for (int rowId = 0; rowId < numRows; rowId++) {
                boolean leftIsTrue = !left.isNullAt(rowId) && left.getBoolean(rowId);
                boolean rightIsTrue = !right.isNullAt(rowId) && right.getBoolean(rowId);
                boolean leftIsFalse = !left.isNullAt(rowId) && !left.getBoolean(rowId);
                boolean rightIsFalse = !right.isNullAt(rowId) && !right.getBoolean(rowId);

                if (leftIsTrue || rightIsTrue) {
                    nullability[rowId] = false;
                    result[rowId] = true;
                } else if (leftIsFalse && rightIsFalse) {
                    nullability[rowId] = false;
                    result[rowId] = false;
                } else {
                    nullability[rowId] = true;
                    // result[rowId] is undefined when nullability[rowId] = true
                }
            }
            return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
        }

        @Override
        ColumnVector visitAlwaysTrue(AlwaysTrue alwaysTrue) {
            return new DefaultConstantVector(BooleanType.BOOLEAN, input.getSize(), true);
        }

        @Override
        ColumnVector visitAlwaysFalse(AlwaysFalse alwaysFalse) {
            return new DefaultConstantVector(BooleanType.BOOLEAN, input.getSize(), false);
        }

        @Override
        ColumnVector visitComparator(Predicate predicate) {
            PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(predicate);

            int numRows = argResults.rowCount;
            boolean[] result = new boolean[numRows];
            boolean[] nullability = evalNullability(argResults.leftResult, argResults.rightResult);
            int[] compareResult = compare(argResults.leftResult, argResults.rightResult);
            switch (predicate.getName()) {
                case "=":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] == 0;
                    }
                    break;
                case ">":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] > 0;
                    }
                    break;
                case ">=":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] >= 0;
                    }
                    break;
                case "<":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] < 0;
                    }
                    break;
                case "<=":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] <= 0;
                    }
                    break;
                default:
                    // We should never reach this based on the ExpressionVisitor
                    throw new IllegalStateException(
                            String.format("%s is not a recognized comparator", predicate.getName()));
            }

            return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
        }

        @Override
        ColumnVector visitLiteral(Literal literal) {
            DataType dataType = literal.getDataType();
            if (dataType instanceof BooleanType || dataType instanceof ByteType || dataType instanceof ShortType
                    || dataType instanceof IntegerType || dataType instanceof LongType || dataType instanceof FloatType
                    || dataType instanceof DoubleType || dataType instanceof StringType
                    || dataType instanceof BinaryType || dataType instanceof DecimalType || dataType instanceof DateType
                    || dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
                return new DefaultConstantVector(dataType, input.getSize(), literal.getValue());
            }

            throw new UnsupportedOperationException("unsupported expression encountered: " + literal);
        }

        @Override
        ColumnVector visitColumn(Column column) {
            String[] names = column.getNames();
            DataType currentType = input.getSchema();
            ColumnVector columnVector = null;
            for (int level = 0; level < names.length; level++) {
                assertColumnExists(currentType instanceof StructType, input.getSchema(), column);
                StructType structSchema = ((StructType) currentType);
                int ordinal = structSchema.indexOf(names[level]);
                assertColumnExists(ordinal != -1, input.getSchema(), column);
                currentType = structSchema.at(ordinal).getDataType();

                if (level == 0) {
                    columnVector = input.getColumnVector(ordinal);
                } else {
                    columnVector = columnVector.getChild(ordinal);
                }
            }
            assertColumnExists(columnVector != null, input.getSchema(), column);
            return columnVector;
        }

        @Override
        ColumnVector visitCast(ImplicitCastExpression cast) {
            ColumnVector inputResult = visit(cast.getInput());
            return cast.eval(inputResult);
        }

        @Override
        ColumnVector visitPartitionValue(PartitionValueExpression partitionValue) {
            ColumnVector input = visit(partitionValue.getInput());
            return PartitionValueEvaluator.eval(input, partitionValue.getDataType());
        }

        @Override
        ColumnVector visitElementAt(ScalarExpression elementAt) {
            ColumnVector map = visit(childAt(elementAt, 0));
            ColumnVector lookupKey = visit(childAt(elementAt, 1));
            return ElementAtEvaluator.eval(map, lookupKey);
        }

        @Override
        ColumnVector visitNot(Predicate predicate) {
            ColumnVector childResult = visit(childAt(predicate, 0));
            return booleanWrapperVector(childResult, rowId -> !childResult.getBoolean(rowId),
                    rowId -> childResult.isNullAt(rowId));
        }

        @Override
        ColumnVector visitIsNotNull(Predicate predicate) {
            ColumnVector childResult = visit(childAt(predicate, 0));
            return booleanWrapperVector(childResult, rowId -> !childResult.isNullAt(rowId), rowId -> false);
        }

        @Override
        ColumnVector visitIsNull(Predicate predicate) {
            ColumnVector childResult = visit(getUnaryChild(predicate));
            return booleanWrapperVector(childResult, rowId -> childResult.isNullAt(rowId), rowId -> false);
        }

        @Override
        ColumnVector visitCoalesce(ScalarExpression coalesce) {
            List<ColumnVector> childResults =
                    coalesce.getChildren().stream().map(this::visit).collect(Collectors.toList());
            return DefaultExpressionUtils.combinationVector(childResults, rowId -> {
                for (int idx = 0; idx < childResults.size(); idx++) {
                    if (!childResults.get(idx).isNullAt(rowId)) {
                        return idx;
                    }
                }
                return 0; // If all are null then any idx suffices
            });
        }

        /**
         * Utility method to evaluate inputs to the binary input expression. Also validates the
         * evaluated expression result {@link ColumnVector}s are of the same size.
         *
         * @param predicate
         * @return Triplet of (result vector size, left operand result, left operand result)
         */
        private PredicateChildrenEvalResult evalBinaryExpressionChildren(Predicate predicate) {
            ColumnVector left = visit(getLeft(predicate));
            ColumnVector right = visit(getRight(predicate));
            checkArgument(left.getSize() == right.getSize(),
                    "Left and right operand returned different results: left=%d, right=d", left.getSize(),
                    right.getSize());
            return new PredicateChildrenEvalResult(left.getSize(), left, right);
        }
    }

    /**
     * Encapsulates children expression result of binary input predicate
     */
    private static class PredicateChildrenEvalResult {
        public final int rowCount;
        public final ColumnVector leftResult;
        public final ColumnVector rightResult;

        PredicateChildrenEvalResult(int rowCount, ColumnVector leftResult, ColumnVector rightResult) {
            this.rowCount = rowCount;
            this.leftResult = leftResult;
            this.rightResult = rightResult;
        }
    }

    private static void assertColumnExists(boolean condition, StructType schema, Column column) {
        if (!condition) {
            throw new IllegalArgumentException(
                    String.format("%s doesn't exist in input data schema: %s", column, schema));
        }
    }

}
