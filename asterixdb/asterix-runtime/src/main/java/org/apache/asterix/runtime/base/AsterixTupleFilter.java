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

package org.apache.asterix.runtime.base;

import org.apache.hyracks.algebricks.data.IBinaryBooleanInspector;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;

public class AsterixTupleFilter implements ITupleFilter {
    private final IBinaryBooleanInspector boolInspector;
    private final IScalarEvaluator eval;
    private final IPointable p = VoidPointable.FACTORY.createPointable();

    public AsterixTupleFilter(IEvaluatorContext ctx, IScalarEvaluatorFactory evalFactory,
            IBinaryBooleanInspector boolInspector) throws HyracksDataException {
        this.boolInspector = boolInspector;
        this.eval = evalFactory.createScalarEvaluator(ctx);
    }

    @Override
    public boolean accept(IFrameTupleReference tuple) throws HyracksDataException {
        eval.evaluate(tuple, p);
        return boolInspector.getBooleanValue(p.getByteArray(), p.getStartOffset(), p.getLength());
    }
}
