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
package org.apache.asterix.om.functions;

import java.util.List;
import java.util.Objects;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public abstract class FunctionInfo implements IFunctionInfo {
    private static final long serialVersionUID = 5460606629941107899L;

    private final FunctionIdentifier functionIdentifier;
    private final transient IResultTypeComputer typeComputer;
    private final boolean isFunctional;

    public FunctionInfo(FunctionIdentifier functionIdentifier, IResultTypeComputer typeComputer, boolean isFunctional) {
        this.functionIdentifier = Objects.requireNonNull(functionIdentifier);
        this.typeComputer = Objects.requireNonNull(typeComputer);
        this.isFunctional = isFunctional;
    }

    @Override
    public FunctionIdentifier getFunctionIdentifier() {
        return functionIdentifier;
    }

    @Override
    public boolean isFunctional() {
        return isFunctional;
    }

    public IResultTypeComputer getResultTypeComputer() {
        return typeComputer;
    }

    /**
     * @param args,
     *            the arguments.
     * @return a display string of the FunctionInfo.
     */
    @Override
    public String display(List<Mutable<ILogicalExpression>> args) {
        return FunctionDisplayUtil.display(this, args, input -> IFunctionInfo.super.display(input));
    }

    @Override
    public String toString() {
        return functionIdentifier.toString();
    }
}
