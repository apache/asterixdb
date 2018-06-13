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

package org.apache.asterix.lang.common.base;

import java.util.List;

/**
 * Interface for statements that can return tuples to users,
 * e.g., QUERY, INSERT, and UPSERT.
 */
public interface IReturningStatement extends Statement {

    /**
     * @return the current variable counter, i.e., the largest used variable id.
     */
    int getVarCounter();

    /**
     * Sets the variable counter to the input value.
     *
     * @param varCounter,
     *            the largest occupied variable id.
     */
    void setVarCounter(int varCounter);

    /**
     * Is the statement a top-level statement or a subquery?
     *
     * @return true if yes, false otherwise.
     */
    boolean isTopLevel();

    /**
     * @return directly enclosed top-level expressions within the statement.
     */
    List<Expression> getDirectlyEnclosedExpressions();

    /**
     * @return the main body expression of the statement.
     */
    Expression getBody();

    /**
     * Sets the main body expression of the statement.
     *
     * @param expr,
     *            the main body expression.
     */
    void setBody(Expression expr);
}
