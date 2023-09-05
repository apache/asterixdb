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

import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public class NoOpExternalFilterEvaluatorFactory implements IExternalFilterEvaluatorFactory {
    public static final IExternalFilterEvaluatorFactory INSTANCE = new NoOpExternalFilterEvaluatorFactory();
    private static final long serialVersionUID = 6470035020297216949L;

    private NoOpExternalFilterEvaluatorFactory() {
    }

    @Override
    public IExternalFilterEvaluator create(IServiceContext serviceContext, IWarningCollector warningCollector) {
        return NoOpExternalFilterEvaluator.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IExternalFilterValueEmbedder createValueEmbedder(IWarningCollector warningCollector) {
        return NoOpFilterValueEmbedder.INSTANCE;
    }

}
