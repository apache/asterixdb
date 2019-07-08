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

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IExternalFunction;
import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ExternalFunctionProvider {

    public static IExternalFunction getExternalFunctionEvaluator(IExternalFunctionInfo finfo,
            IScalarEvaluatorFactory[] args, IEvaluatorContext context, IApplicationContext appCtx)
            throws HyracksDataException {
        switch (finfo.getKind()) {
            case SCALAR:
                return new ExternalScalarFunction(finfo, args, context, appCtx);
            case AGGREGATE:
            case UNNEST:
                throw new RuntimeDataException(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND, finfo.getKind());
            default:
                throw new RuntimeDataException(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNKNOWN_KIND, finfo.getKind());
        }
    }
}

class ExternalScalarFunction extends ExternalFunction implements IExternalScalarFunction, IScalarEvaluator {

    public ExternalScalarFunction(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IEvaluatorContext context, IApplicationContext appCtx) throws HyracksDataException {
        super(finfo, args, context, appCtx);
        try {
            initialize(functionHelper);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
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
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void evaluate(IFunctionHelper argumentProvider) throws HyracksDataException {
        try {
            resultBuffer.reset();
            ((IExternalScalarFunction) externalFunctionInstance).evaluate(argumentProvider);
            if (!argumentProvider.isValidResult()) {
                throw new RuntimeDataException(ErrorCode.EXTERNAL_UDF_RESULT_TYPE_ERROR);
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

}
