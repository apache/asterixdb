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
package org.apache.hyracks.algebricks.runtime.evaluators;

import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ColumnAccessEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private final int fieldIndex;

    public ColumnAccessEvalFactory(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    @Override
    public String toString() {
        return "ColumnAccess(" + fieldIndex + ")";
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
        return new IScalarEvaluator() {

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                byte[] buffer = tuple.getFieldData(fieldIndex);
                int start = tuple.getFieldStart(fieldIndex);
                int length = tuple.getFieldLength(fieldIndex);
                result.set(buffer, start, length);
            }
        };
    }

}
