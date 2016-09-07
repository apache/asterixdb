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
package org.apache.hyracks.algebricks.core.algebra.typing;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * Implementations of this interface are supposed to be in charge of type inferences
 * during query compilations.
 */
public interface ITypingContext {

    /**
     * Gets the type environment from the output perspective of the argument operator.
     *
     * @param op,
     *            the operator of interests.
     * @return the type environment after the operator's processing.
     */
    public IVariableTypeEnvironment getOutputTypeEnvironment(ILogicalOperator op);

    /**
     * Sets the output type environment of an operator.
     *
     * @param op,
     *            the operator of interests.
     * @param env,
     *            the type environment after the operator's processing.
     */
    public void setOutputTypeEnvironment(ILogicalOperator op, IVariableTypeEnvironment env);

    /**
     * @return the type computer for expressions.
     */
    public IExpressionTypeComputer getExpressionTypeComputer();

    /**
     * @return a type computer for "missable" types, e.g.,
     *         the resulting types for variables populated from the right input branch of
     *         a left outer join.
     */
    public IMissableTypeComputer getMissableTypeComputer();

    /**
     * @return a resolver for conflicting types.
     */
    public IConflictingTypeResolver getConflictingTypeResolver();

    /**
     * @return the metadata provider, which is in charge of metadata reads/writes.
     */
    public IMetadataProvider<?, ?> getMetadataProvider();

    /**
     * Invalidates a type environment for an operator.
     *
     * @param op,
     *            the operator of interests.
     */
    public void invalidateTypeEnvironmentForOperator(ILogicalOperator op);

    /**
     * (Re-)computes and sets a type environment for an operator.
     *
     * @param op
     *            the operator of interests.
     * @throws AlgebricksException
     */
    public void computeAndSetTypeEnvironmentForOperator(ILogicalOperator op) throws AlgebricksException;

}
