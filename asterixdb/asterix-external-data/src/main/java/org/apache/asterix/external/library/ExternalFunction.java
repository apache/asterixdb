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

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IExternalFunction;
import org.apache.asterix.external.api.IFunctionFactory;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.util.AsterixAppContextInfo;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class ExternalFunction implements IExternalFunction {

    protected final IExternalFunctionInfo finfo;
    protected final IFunctionFactory externalFunctionFactory;
    protected final IExternalFunction externalFunction;
    protected final IScalarEvaluatorFactory[] evaluatorFactories;
    protected final IPointable inputVal = new VoidPointable();
    protected final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    protected final ArrayBackedValueStorage castBuffer = new ArrayBackedValueStorage();
    protected final IScalarEvaluator[] argumentEvaluators;
    protected final JavaFunctionHelper functionHelper;

    public ExternalFunction(IExternalFunctionInfo finfo, IScalarEvaluatorFactory args[], IHyracksTaskContext context)
            throws HyracksDataException {
        this.finfo = finfo;
        this.evaluatorFactories = args;
        argumentEvaluators = new IScalarEvaluator[args.length];
        for (int i = 0; i < args.length; i++) {
            argumentEvaluators[i] = args[i].createScalarEvaluator(context);
        }
        functionHelper = new JavaFunctionHelper(finfo, resultBuffer);

        String[] fnameComponents = finfo.getFunctionIdentifier().getName().split("#");
        String functionLibary = fnameComponents[0];
        String dataverse = finfo.getFunctionIdentifier().getNamespace();
        ILibraryManager libraryManager;
        if (context == null) {
            // Gets the library manager for compile-time constant folding.
            libraryManager = AsterixAppContextInfo.INSTANCE.getLibraryManager();
        } else {
            // Gets the library manager for real runtime evaluation.
            IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) context.getJobletContext()
                    .getApplicationContext().getApplicationObject();
            libraryManager = runtimeCtx.getLibraryManager();
        }
        ClassLoader libraryClassLoader = libraryManager.getLibraryClassLoader(dataverse, functionLibary);
        String classname = finfo.getFunctionBody().trim();
        Class<?> clazz;
        try {
            clazz = Class.forName(classname, true, libraryClassLoader);
            externalFunctionFactory = (IFunctionFactory) clazz.newInstance();
            externalFunction = externalFunctionFactory.getExternalFunction();
        } catch (Exception e) {
            throw new HyracksDataException(" Unable to load/instantiate class " + classname, e);
        }
    }

    public static ISerializerDeserializer<?> getSerDe(Object typeInfo) {
        return AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(typeInfo);
    }

    public IExternalFunctionInfo getFinfo() {
        return finfo;
    }

    public void setArguments(IFrameTupleReference tuple) throws AlgebricksException, IOException, AsterixException {
        for (int i = 0; i < evaluatorFactories.length; i++) {
            argumentEvaluators[i].evaluate(tuple, inputVal);

            // Type-cast the source array based on the input type that this function wants to receive.
            ATypeTag targetTypeTag = finfo.getParamList().get(i).getTypeTag();
            ATypeTag sourceTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]);
            if (sourceTypeTag != targetTypeTag) {
                castBuffer.reset();
                ATypeHierarchy.convertNumericTypeByteArray(inputVal.getByteArray(), inputVal.getStartOffset(),
                        inputVal.getLength(), targetTypeTag, castBuffer.getDataOutput());
                functionHelper.setArgument(i, castBuffer);
            } else {
                functionHelper.setArgument(i, inputVal);
            }
        }
    }

    @Override
    public void deinitialize() {
        externalFunction.deinitialize();
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        externalFunction.initialize(functionHelper);
    }

}
