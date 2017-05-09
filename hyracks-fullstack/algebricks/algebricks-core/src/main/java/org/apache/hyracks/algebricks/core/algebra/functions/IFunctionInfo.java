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
package org.apache.hyracks.algebricks.core.algebra.functions;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public interface IFunctionInfo extends Serializable {
    /**
     * @return the FunctionIdentifier.
     */
    FunctionIdentifier getFunctionIdentifier();

    /**
     * @return true if the function is a stateful function; false otherwise.
     */
    default boolean isFunctional() {
        // A function is functional by default.
        return true;
    }

    /**
     * @param args,
     *            the arguments.
     * @return a display string of the FunctionInfo.
     */
    default String display(List<Mutable<ILogicalExpression>> args) {
        StringBuilder sb = new StringBuilder();
        sb.append(getFunctionIdentifier().getName() + "(");
        boolean first = true;
        for (Mutable<ILogicalExpression> ref : args) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(ref.getValue());
        }
        sb.append(")");
        return sb.toString();
    }
}
