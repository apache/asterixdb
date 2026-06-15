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
package org.apache.asterix.common.cache;

import java.util.Set;

import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.api.exceptions.Warning;

/**
 * A cached compiled plan, stored as the {@link IQueryPlanCache} map value: the optimized logical plan, its
 * optimization context, and the compile-time warnings to replay on a hit.
 */
public interface IQueryPlanCacheValue {

    /**
     * @return the optimized logical plan to reuse on a cache hit
     */
    ILogicalPlan plan();

    /**
     * @return the compile-time warnings to replay on a cache hit
     */
    Set<Warning> warnings();

    /**
     * @return the optimization context captured when the plan was compiled
     */
    IOptimizationContext optContext();
}
