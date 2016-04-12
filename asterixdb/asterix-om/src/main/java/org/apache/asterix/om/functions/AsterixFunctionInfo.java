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

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.hyracks.algebricks.core.algebra.functions.AbstractFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class AsterixFunctionInfo extends AbstractFunctionInfo {

    private static final long serialVersionUID = 1L;

    private final FunctionIdentifier functionIdentifier;

    public AsterixFunctionInfo(String namespace, AsterixFunction asterixFunction, boolean isFunctional) {
        super(isFunctional);
        this.functionIdentifier = new FunctionIdentifier(namespace, asterixFunction.getName(),
                asterixFunction.getArity());
    }

    public AsterixFunctionInfo() {
        super(true);
        functionIdentifier = null;
    }

    public AsterixFunctionInfo(FunctionIdentifier functionIdentifier, boolean isFunctional) {
        super(isFunctional);
        this.functionIdentifier = functionIdentifier;
    }

    public AsterixFunctionInfo(FunctionSignature functionSignature, boolean isFunctional) {
        super(isFunctional);
        this.functionIdentifier = new FunctionIdentifier(functionSignature.getNamespace(), functionSignature.getName(),
                functionSignature.getArity());
    }

    @Override
    public FunctionIdentifier getFunctionIdentifier() {
        return functionIdentifier;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AsterixFunctionInfo)) {
            return false;
        }
        AsterixFunctionInfo info = (AsterixFunctionInfo) o;
        return functionIdentifier.equals(info.getFunctionIdentifier())
                && functionIdentifier.getArity() == info.getFunctionIdentifier().getArity();
    }

    @Override
    public String toString() {
        return this.functionIdentifier.getNamespace() + ":" + this.functionIdentifier.getName() + "@"
                + this.functionIdentifier.getArity();
    }

}
