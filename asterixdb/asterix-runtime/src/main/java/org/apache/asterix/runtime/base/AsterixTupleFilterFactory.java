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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspector;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

public class AsterixTupleFilterFactory implements ITupleFilterFactory {

    private static final long serialVersionUID = 1L;

    private final IBinaryBooleanInspectorFactory boolInspectorFactory;
    private final IScalarEvaluatorFactory evalFactory;

    public AsterixTupleFilterFactory(IScalarEvaluatorFactory evalFactory,
            IBinaryBooleanInspectorFactory boolInspectorFactory) throws AlgebricksException {
        this.evalFactory = evalFactory;
        this.boolInspectorFactory = boolInspectorFactory;
    }

    @Override
    public ITupleFilter createTupleFilter(IHyracksTaskContext ctx) throws HyracksDataException {
        IEvaluatorContext evalCtx = new EvaluatorContext(ctx);
        IBinaryBooleanInspector bbi = boolInspectorFactory.createBinaryBooleanInspector(ctx);
        return new AsterixTupleFilter(evalCtx, evalFactory, bbi);
    }
}
