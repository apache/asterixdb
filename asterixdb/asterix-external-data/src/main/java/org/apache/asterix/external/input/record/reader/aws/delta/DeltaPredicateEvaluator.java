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

import java.util.Optional;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultConstantVector;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

public class DeltaPredicateEvaluator implements PredicateEvaluator {

    private final ExpressionEvaluator expressionEvaluator;
    private static final String EXISTING_SEL_VECTOR_COL_NAME = "____existing_selection_vector_value____";
    private static final StructField EXISTING_SEL_VECTOR_FIELD =
            new StructField(EXISTING_SEL_VECTOR_COL_NAME, BooleanType.BOOLEAN, false);

    public DeltaPredicateEvaluator(StructType inputSchema, Predicate predicate) {
        Predicate rewrittenPredicate = new And(
                new Predicate("=", new Column(EXISTING_SEL_VECTOR_COL_NAME), Literal.ofBoolean(true)), predicate);
        StructType rewrittenInputSchema = inputSchema.add(EXISTING_SEL_VECTOR_FIELD);
        this.expressionEvaluator =
                new DeltaExpressionEvaluator(rewrittenInputSchema, rewrittenPredicate, BooleanType.BOOLEAN);
    }

    @Override
    public ColumnVector eval(ColumnarBatch inputData, Optional<ColumnVector> existingSelectionVector) {
        try {
            ColumnVector newVector = existingSelectionVector
                    .orElse(new DefaultConstantVector(BooleanType.BOOLEAN, inputData.getSize(), true));
            ColumnarBatch withExistingSelVector =
                    inputData.withNewColumn(inputData.getSchema().length(), EXISTING_SEL_VECTOR_FIELD, newVector);

            return expressionEvaluator.eval(withExistingSelVector);
        } finally {
            // release the existing selection vector.
            Utils.closeCloseables(existingSelectionVector.orElse(null));
        }
    }
}
