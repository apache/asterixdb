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

import java.util.Arrays;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.IWarningCollector;

class FilterEvaluatorContext extends EvaluatorContext {
    private final IExternalFilterValueEvaluator[] evaluators;

    public FilterEvaluatorContext(IServiceContext serviceContext, IWarningCollector warningCollector,
            IExternalFilterValueEvaluator[] evaluators) {
        super(serviceContext, warningCollector);
        this.evaluators = evaluators;
        Arrays.fill(evaluators, NoOpExternalFilterValueEvaluator.INSTANCE);
    }

    public IScalarEvaluator createEvaluator(int index, ATypeTag typeTag) {
        IExternalFilterValueEvaluator evaluator = evaluators[index];
        if (evaluator == NoOpExternalFilterValueEvaluator.INSTANCE) {
            evaluator = new ExternalFilterValueEvaluator(typeTag);
            evaluators[index] = evaluator;
        }
        return evaluator;
    }
}
