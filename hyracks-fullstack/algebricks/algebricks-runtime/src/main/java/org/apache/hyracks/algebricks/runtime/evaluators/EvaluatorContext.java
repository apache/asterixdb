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

import java.util.Objects;

import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public final class EvaluatorContext implements IEvaluatorContext {

    private final IServiceContext serviceContext;

    private final IHyracksTaskContext taskContext;

    private final IWarningCollector warningCollector;

    public EvaluatorContext(IHyracksTaskContext taskContext) {
        this.taskContext = Objects.requireNonNull(taskContext);
        this.serviceContext = Objects.requireNonNull(taskContext.getJobletContext().getServiceContext());
        this.warningCollector = Objects.requireNonNull(taskContext.getWarningCollector());
    }

    public EvaluatorContext(IServiceContext serviceContext, IWarningCollector warningCollector) {
        this.taskContext = null;
        this.serviceContext = Objects.requireNonNull(serviceContext);
        this.warningCollector = Objects.requireNonNull(warningCollector);
    }

    public EvaluatorContext(IHyracksTaskContext taskContext, IWarningCollector warningCollector) {
        this.taskContext = Objects.requireNonNull(taskContext);
        this.serviceContext = Objects.requireNonNull(taskContext.getJobletContext().getServiceContext());
        this.warningCollector = Objects.requireNonNull(warningCollector);
    }

    @Override
    public IServiceContext getServiceContext() {
        return serviceContext;
    }

    @Override
    public IHyracksTaskContext getTaskContext() {
        return taskContext;
    }

    @Override
    public IWarningCollector getWarningCollector() {
        return warningCollector;
    }
}
