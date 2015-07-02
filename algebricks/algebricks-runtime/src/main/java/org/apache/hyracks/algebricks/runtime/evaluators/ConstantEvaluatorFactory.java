/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hyracks.algebricks.runtime.evaluators;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ConstantEvaluatorFactory implements IScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private byte[] value;

    public ConstantEvaluatorFactory(byte[] value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Constant";
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
        return new IScalarEvaluator() {
            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                result.set(value, 0, value.length);
            }
        };
    }

}