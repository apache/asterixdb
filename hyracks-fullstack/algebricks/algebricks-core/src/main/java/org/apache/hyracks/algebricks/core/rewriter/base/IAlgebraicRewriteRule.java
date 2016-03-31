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
package org.apache.hyracks.algebricks.core.rewriter.base;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;

public interface IAlgebraicRewriteRule {

    /**
     * This method is invoked in pre-order traversal of a query plan.
     *
     * @param opRef,
     *            the reference of the current operator to look at,
     * @param context
     *            the optimization context
     * @return true if any change is introduced to the query plan; false otherwise.
     * @throws AlgebricksException
     */
    public default boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    /**
     * This method is invoked in the post-order traversal of a query plan.
     *
     * @param opRef,
     *            the reference of the current operator to look at,
     * @param context
     *            the optimization context
     * @return true if any change is introduced to the query plan; false otherwise.
     * @throws AlgebricksException
     */
    public default boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }
}
