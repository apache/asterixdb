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

import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public class ExternalFilterEvaluatorFactory implements IExternalFilterEvaluatorFactory {
    private static final long serialVersionUID = -309935877927008746L;
    private final int numberOfComputedFields;
    private final IScalarEvaluatorFactory filterEvalFactory;

    public ExternalFilterEvaluatorFactory(int numberOfComputedFields, IScalarEvaluatorFactory filterEvalFactory) {
        this.numberOfComputedFields = numberOfComputedFields;
        this.filterEvalFactory = filterEvalFactory;
    }

    @Override
    public IExternalFilterEvaluator create(IServiceContext serviceContext, IWarningCollector warningCollector)
            throws HyracksDataException {
        IExternalFilterValueEvaluator[] valueEvaluators = new IExternalFilterValueEvaluator[numberOfComputedFields];
        Arrays.fill(valueEvaluators, NoOpExternalFilterValueEvaluator.INSTANCE);
        FilterEvaluatorContext filterContext =
                new FilterEvaluatorContext(serviceContext, warningCollector, valueEvaluators);
        return new ExternalFilterEvaluator(filterEvalFactory.createScalarEvaluator(filterContext), valueEvaluators);
    }
}
