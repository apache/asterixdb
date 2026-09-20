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
package org.apache.asterix.external.input.filter;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.asterix.om.types.ATypeTag;

/**
 * A row-group filter that names columns and literals without committing to a physical Parquet type.
 * <p>
 * The filter is built while compiling the query, where only the AsterixDB type of each literal is known, and is
 * turned into a parquet-mr {@code FilterPredicate} by {@link ParquetFilterConverter} on the node, once the schema
 * of the file about to be read is in hand. The physical type cannot be chosen any earlier: a collection spans
 * files whose schemas may disagree, and the physical type of a column decides which {@code FilterApi} column a
 * predicate has to use. A predicate built against the wrong one is rejected by parquet-mr when the file is
 * opened, which fails the query rather than skipping the optimization.
 */
public abstract class ParquetFilterExpression implements Serializable {
    private static final long serialVersionUID = 1L;

    private ParquetFilterExpression() {
    }

    /**
     * The comparisons that can be pushed. Parquet evaluates these against a row group's statistics, so only
     * operators with a total order on the column's values are useful here.
     */
    public enum Operator {
        EQ,
        GT,
        GT_EQ,
        LT,
        LT_EQ
    }

    public static final class And extends ParquetFilterExpression {
        private static final long serialVersionUID = 1L;

        private final ParquetFilterExpression left;
        private final ParquetFilterExpression right;

        public And(ParquetFilterExpression left, ParquetFilterExpression right) {
            this.left = left;
            this.right = right;
        }

        public ParquetFilterExpression getLeft() {
            return left;
        }

        public ParquetFilterExpression getRight() {
            return right;
        }

        @Override
        public String toString() {
            return "and(" + left + ", " + right + ')';
        }
    }

    public static final class Or extends ParquetFilterExpression {
        private static final long serialVersionUID = 1L;

        private final ParquetFilterExpression left;
        private final ParquetFilterExpression right;

        public Or(ParquetFilterExpression left, ParquetFilterExpression right) {
            this.left = left;
            this.right = right;
        }

        public ParquetFilterExpression getLeft() {
            return left;
        }

        public ParquetFilterExpression getRight() {
            return right;
        }

        @Override
        public String toString() {
            return "or(" + left + ", " + right + ')';
        }
    }

    /**
     * A comparison between one column and one literal.
     * <p>
     * The column is held as its path components rather than a dotted string: parquet-mr splits a dotted string
     * on every '.', so a field whose own name contains one would resolve to a nested path that does not exist,
     * and a predicate on a column parquet-mr cannot find excludes every row group without reporting anything.
     */
    public static final class Comparison extends ParquetFilterExpression {
        private static final long serialVersionUID = 1L;

        private final String[] path;
        private final Operator operator;
        private final ATypeTag valueTag;
        private final Object value;

        /**
         * @param path     the column's path components, outermost first
         * @param operator the comparison, read left to right as {@code column <operator> value}
         * @param valueTag the AsterixDB type of the literal
         * @param value    the literal, as {@link Long}, {@link Double}, {@link String} or {@link Boolean}
         */
        public Comparison(String[] path, Operator operator, ATypeTag valueTag, Object value) {
            this.path = path;
            this.operator = operator;
            this.valueTag = valueTag;
            this.value = value;
        }

        public String[] getPath() {
            return path;
        }

        public Operator getOperator() {
            return operator;
        }

        public ATypeTag getValueTag() {
            return valueTag;
        }

        public Object getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.join(".", path) + ' ' + operator + ' ' + valueTag + '(' + value + ')';
        }
    }

    /**
     * Mirrors the shape of this expression without its literals, for logging that must not disclose user data.
     */
    public static String describeShape(ParquetFilterExpression expression) {
        if (expression instanceof And) {
            And and = (And) expression;
            return "and(" + describeShape(and.getLeft()) + ", " + describeShape(and.getRight()) + ')';
        } else if (expression instanceof Or) {
            Or or = (Or) expression;
            return "or(" + describeShape(or.getLeft()) + ", " + describeShape(or.getRight()) + ')';
        } else if (expression instanceof Comparison) {
            Comparison comparison = (Comparison) expression;
            return Arrays.toString(comparison.getPath()) + ' ' + comparison.getOperator() + ' '
                    + comparison.getValueTag();
        }
        return String.valueOf(expression);
    }
}
