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

package org.apache.hyracks.api.context;

import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.IWarningCollector;

/**
 * Context for runtime function evaluators
 */
public interface IEvaluatorContext {
    /**
     * Returns service context. Available at compile time
     * (CC context) and at run time (NC context).
     */
    IServiceContext getServiceContext();

    /**
     * Returns current task's context, or {@code null} if this evaluator
     * is being executed by the constant folding rule at compile time.
     */
    IHyracksTaskContext getTaskContext();

    /**
     * Returns a warning collector, never {@code null}
     */
    IWarningCollector getWarningCollector();
}
