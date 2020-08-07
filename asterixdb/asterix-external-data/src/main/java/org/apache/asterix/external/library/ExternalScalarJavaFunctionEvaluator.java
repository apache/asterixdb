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

import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionFactory;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

class ExternalScalarJavaFunctionEvaluator extends ExternalScalarFunctionEvaluator {

    private final IExternalScalarFunction externalFunctionInstance;
    private final IPointable inputVal = VoidPointable.FACTORY.createPointable();
    private final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    protected final JavaFunctionHelper functionHelper;

    ExternalScalarJavaFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args, IAType[] argTypes,
            IEvaluatorContext context) throws HyracksDataException {
        super(finfo, args, argTypes, context);

        DataverseName libraryDataverseName = finfo.getLibraryDataverseName();
        String libraryName = finfo.getLibraryName();
        JavaLibrary library = (JavaLibrary) libraryManager.getLibrary(libraryDataverseName, libraryName);

        String classname = finfo.getExternalIdentifier().get(0);
        try {
            Class<?> clazz = Class.forName(classname, true, library.getClassLoader());
            IFunctionFactory externalFunctionFactory = (IFunctionFactory) clazz.newInstance();
            externalFunctionInstance = (IExternalScalarFunction) externalFunctionFactory.getExternalFunction();
        } catch (Exception e) {
            throw new RuntimeDataException(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNABLE_TO_LOAD_CLASS, e, classname);
        }

        functionHelper = new JavaFunctionHelper(finfo, argTypes, resultBuffer);
        try {
            externalFunctionInstance.initialize(functionHelper);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        try {
            setArguments(tuple);
            resultBuffer.reset();
            externalFunctionInstance.evaluate(functionHelper);
            if (!functionHelper.isValidResult()) {
                throw new RuntimeDataException(ErrorCode.EXTERNAL_UDF_RESULT_TYPE_ERROR);
            }
            result.set(resultBuffer.getByteArray(), resultBuffer.getStartOffset(), resultBuffer.getLength());
            functionHelper.reset();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    public void setArguments(IFrameTupleReference tuple) throws IOException {
        for (int i = 0; i < argEvals.length; i++) {
            argEvals[i].evaluate(tuple, inputVal);
            functionHelper.setArgument(i, inputVal);
        }
    }
}
