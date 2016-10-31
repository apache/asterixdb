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
package org.apache.asterix.external.library;

import org.apache.asterix.external.api.IExternalFunction;
import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ExternalFunctionProvider {

    public static IExternalFunction getExternalFunctionEvaluator(IExternalFunctionInfo finfo,
            IScalarEvaluatorFactory args[], IHyracksTaskContext context) throws HyracksDataException {
        switch (finfo.getKind()) {
            case SCALAR:
                return new ExternalScalarFunction(finfo, args, context);
            case AGGREGATE:
            case UNNEST:
                throw new IllegalArgumentException(" UDF of kind" + finfo.getKind() + " not supported.");
            default:
                throw new IllegalArgumentException(" unknown function kind" + finfo.getKind());
        }
    }
}

class ExternalScalarFunction extends ExternalFunction implements IExternalScalarFunction, IScalarEvaluator {

    public ExternalScalarFunction(IExternalFunctionInfo finfo, IScalarEvaluatorFactory args[],
            IHyracksTaskContext context) throws HyracksDataException {
        super(finfo, args, context);
        try {
            initialize(functionHelper);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        try {
            setArguments(tuple);
            evaluate(functionHelper);
            result.set(resultBuffer.getByteArray(), resultBuffer.getStartOffset(), resultBuffer.getLength());
            functionHelper.reset();
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void evaluate(IFunctionHelper argumentProvider) throws HyracksDataException {
        try {
            resultBuffer.reset();
            ((IExternalScalarFunction) externalFunction).evaluate(argumentProvider);
        /*
         * Make sure that if "setResult" is not called,
         * or the result object is missing we let Hyracks storage manager know
         * we want to discard a missing object
         */
            byte byteOutput = resultBuffer.getByteArray()[0];
            if (!argumentProvider.isValidResult() || byteOutput == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                resultBuffer.getDataOutput().writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

}
