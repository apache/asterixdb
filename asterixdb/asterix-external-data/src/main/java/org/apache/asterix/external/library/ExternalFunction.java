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

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IExternalFunction;
import org.apache.asterix.external.api.IFunctionFactory;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class ExternalFunction implements IExternalFunction {

    protected final IExternalFunctionInfo finfo;
    protected final IFunctionFactory externalFunctionFactory;
    protected final IExternalFunction externalFunctionInstance;
    protected final IScalarEvaluatorFactory[] evaluatorFactories;
    protected final IPointable inputVal = new VoidPointable();
    protected final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    protected final IScalarEvaluator[] argumentEvaluators;
    protected final JavaFunctionHelper functionHelper;

    public ExternalFunction(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            IApplicationContext appCtx) throws HyracksDataException {
        this.finfo = finfo;
        this.evaluatorFactories = args;
        argumentEvaluators = new IScalarEvaluator[args.length];
        for (int i = 0; i < args.length; i++) {
            argumentEvaluators[i] = args[i].createScalarEvaluator(context);
        }

        ILibraryManager libraryManager = appCtx.getLibraryManager();
        String[] fnameComponents = finfo.getFunctionIdentifier().getName().split("#");
        String functionLibary = fnameComponents[0];
        String dataverse = finfo.getFunctionIdentifier().getNamespace();

        functionHelper = new JavaFunctionHelper(finfo, resultBuffer,
                libraryManager.getFunctionParameters(dataverse, finfo.getFunctionIdentifier().getName()));
        ClassLoader libraryClassLoader = libraryManager.getLibraryClassLoader(dataverse, functionLibary);
        String classname = finfo.getFunctionBody().trim();
        Class<?> clazz;
        try {
            clazz = Class.forName(classname, true, libraryClassLoader);
            externalFunctionFactory = (IFunctionFactory) clazz.newInstance();
            externalFunctionInstance = externalFunctionFactory.getExternalFunction();
        } catch (Exception e) {
            throw new RuntimeDataException(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNABLE_TO_LOAD_CLASS, e, classname);
        }
    }

    public void setArguments(IFrameTupleReference tuple) throws AlgebricksException, IOException {
        for (int i = 0; i < evaluatorFactories.length; i++) {
            argumentEvaluators[i].evaluate(tuple, inputVal);
            functionHelper.setArgument(i, inputVal);
        }
    }

    @Override
    public void deinitialize() {
        externalFunctionInstance.deinitialize();
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        externalFunctionInstance.initialize(functionHelper);
    }

}
