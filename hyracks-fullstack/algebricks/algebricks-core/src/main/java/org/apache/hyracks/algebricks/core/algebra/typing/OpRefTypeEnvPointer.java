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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

public class OpRefTypeEnvPointer implements ITypeEnvPointer {

    private final Mutable<ILogicalOperator> op;
    private final ITypingContext ctx;

    public OpRefTypeEnvPointer(Mutable<ILogicalOperator> op, ITypingContext ctx) {
        this.op = op;
        this.ctx = ctx;
    }

    @Override
    public IVariableTypeEnvironment getTypeEnv() {
        return ctx.getOutputTypeEnvironment(op.getValue());
    }

    @Override
    public String toString() {
        return this.getClass().getName() + ":" + op;
    }

}
