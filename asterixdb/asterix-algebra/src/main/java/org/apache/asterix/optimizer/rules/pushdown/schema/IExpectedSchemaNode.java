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
package org.apache.asterix.optimizer.rules.pushdown.schema;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * An interface of the expected schema of value access expressions in a query.
 */
public interface IExpectedSchemaNode {

    /**
     * @return node type
     */
    ExpectedSchemaNodeType getType();

    /**
     * @return source location of the value access
     */
    SourceLocation getSourceLocation();

    /**
     * @return value access function name
     */
    String getFunctionName();

    /**
     * @return the parent of a node
     */
    AbstractComplexExpectedSchemaNode getParent();

    /**
     * Set parent of a node
     *
     * @param parent new parent
     */
    void setParent(AbstractComplexExpectedSchemaNode parent);

    /**
     * For visiting a node
     *
     * @param visitor schema node visitor
     * @param arg     any argument might be needed by the visitor
     * @param <R>     return type
     * @param <T>     argument type
     */
    <R, T> R accept(IExpectedSchemaNodeVisitor<R, T> visitor, T arg);

    /**
     * @return checks whether a node can be replaced
     */
    boolean allowsReplacing();

    /**
     * Replace a node from one type to another
     * Example:
     * - {@link ExpectedSchemaNodeType#ANY} to {@link ExpectedSchemaNodeType#OBJECT}
     * - {@link ExpectedSchemaNodeType#OBJECT} to {@link ExpectedSchemaNodeType#UNION}
     *
     * @param expectedNodeType what is the other expected type
     * @param sourceLocation   source location of the value access
     * @param functionName     function name as in {@link FunctionIdentifier#getName()}
     * @see AbstractComplexExpectedSchemaNode#replaceIfNeeded(ExpectedSchemaNodeType, SourceLocation, String)
     * @see UnionExpectedSchemaNode#replaceIfNeeded(ExpectedSchemaNodeType, SourceLocation, String)
     */
    IExpectedSchemaNode replaceIfNeeded(ExpectedSchemaNodeType expectedNodeType, SourceLocation sourceLocation,
            String functionName);
}
