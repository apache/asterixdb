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

import java.io.Serializable;

import org.apache.asterix.common.functions.FunctionDescriptorTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ISerializedAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.api.exceptions.SourceLocation;

public interface IFunctionDescriptor extends Serializable {

    void setImmutableStates(Object... states);

    void setSourceLocation(SourceLocation sourceLoc);

    FunctionIdentifier getIdentifier();

    FunctionDescriptorTag getFunctionDescriptorTag();

    IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) throws AlgebricksException;

    IRunningAggregateEvaluatorFactory createRunningAggregateEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException;

    ISerializedAggregateEvaluatorFactory createSerializableAggregateEvaluatorFactory(
            final IScalarEvaluatorFactory[] args) throws AlgebricksException;

    IUnnestingEvaluatorFactory createUnnestingEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException;

    IAggregateEvaluatorFactory createAggregateEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException;

}
