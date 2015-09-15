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

import org.apache.asterix.common.functions.FunctionDescriptorTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;

public abstract class AbstractFunctionDescriptor implements IFunctionDescriptor {

    private static final long serialVersionUID = 1L;

    @Override
    public abstract FunctionIdentifier getIdentifier();

    @Override
    public abstract FunctionDescriptorTag getFunctionDescriptorTag();

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(ICopyEvaluatorFactory[] args) throws AlgebricksException {
        throw new NotImplementedException("Not Implemented");
    }

    @Override
    public ICopyRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        throw new NotImplementedException("Not Implemented");
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            ICopyEvaluatorFactory[] args) throws AlgebricksException {
        throw new NotImplementedException("Not Implemented");
    }

    @Override
    public ICopyUnnestingFunctionFactory createUnnestingFunctionFactory(ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        throw new NotImplementedException("Not Implemented");
    }

    @Override
    public ICopyAggregateFunctionFactory createAggregateFunctionFactory(ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        throw new NotImplementedException("Not Implemented");
    }

}
