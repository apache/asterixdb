/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.functions;

import java.io.Serializable;

import edu.uci.ics.asterix.common.functions.FunctionDescriptorTag;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;

public interface IFunctionDescriptor extends Serializable {
    public FunctionIdentifier getIdentifier();

    public FunctionDescriptorTag getFunctionDescriptorTag();

    public ICopyEvaluatorFactory createEvaluatorFactory(ICopyEvaluatorFactory[] args) throws AlgebricksException;

    public ICopyRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException;

    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            final ICopyEvaluatorFactory[] args) throws AlgebricksException;

    public ICopyUnnestingFunctionFactory createUnnestingFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException;

    public ICopyAggregateFunctionFactory createAggregateFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException;

}