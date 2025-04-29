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

import io.delta.kernel.defaults.engine.DefaultExpressionHandler;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

public class DeltaExpressionHandler extends DefaultExpressionHandler {

    @Override
    public ExpressionEvaluator getEvaluator(StructType inputSchema, Expression expression, DataType outputType) {
        return new DeltaExpressionEvaluator(inputSchema, expression, outputType);
    }

    @Override
    public PredicateEvaluator getPredicateEvaluator(StructType inputSchema, Predicate predicate) {
        return new DeltaPredicateEvaluator(inputSchema, predicate);
    }
}
