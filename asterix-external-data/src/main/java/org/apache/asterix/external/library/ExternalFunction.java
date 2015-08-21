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
package edu.uci.ics.asterix.external.library;

import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.functions.ExternalLibraryManager;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.hierachy.ATypeHierarchy;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class ExternalFunction implements IExternalFunction {

    protected final IExternalFunctionInfo finfo;
    protected final IFunctionFactory externalFunctionFactory;
    protected final IExternalFunction externalFunction;
    protected final ICopyEvaluatorFactory[] evaluatorFactories;
    protected final IDataOutputProvider out;
    protected final ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
    protected final ArrayBackedValueStorage castBuffer = new ArrayBackedValueStorage();
    protected final ICopyEvaluator[] argumentEvaluators;
    protected final JavaFunctionHelper functionHelper;

    public ExternalFunction(IExternalFunctionInfo finfo, ICopyEvaluatorFactory args[],
            IDataOutputProvider outputProvider) throws AlgebricksException {
        this.finfo = finfo;
        this.evaluatorFactories = args;
        this.out = outputProvider;
        argumentEvaluators = new ICopyEvaluator[args.length];
        for (int i = 0; i < args.length; i++) {
            argumentEvaluators[i] = args[i].createEvaluator(inputVal);
        }
        functionHelper = new JavaFunctionHelper(finfo, outputProvider);

        String[] fnameComponents = finfo.getFunctionIdentifier().getName().split("#");
        String functionLibary = fnameComponents[0];
        String dataverse = finfo.getFunctionIdentifier().getNamespace();
        ClassLoader libraryClassLoader = ExternalLibraryManager.getLibraryClassLoader(dataverse, functionLibary);
        String classname = finfo.getFunctionBody().trim();
        Class clazz;
        try {
            clazz = Class.forName(classname, true, libraryClassLoader);
            externalFunctionFactory = (IFunctionFactory) clazz.newInstance();
            externalFunction = externalFunctionFactory.getExternalFunction();
        } catch (Exception e) {
            throw new AlgebricksException(" Unable to load/instantiate class " + classname, e);
        }
    }

    public static ISerializerDeserializer getSerDe(Object typeInfo) {
        return AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(typeInfo);
    }

    public IExternalFunctionInfo getFinfo() {
        return finfo;
    }

    public void setArguments(IFrameTupleReference tuple) throws AlgebricksException, IOException, AsterixException {
        for (int i = 0; i < evaluatorFactories.length; i++) {
            inputVal.reset();
            argumentEvaluators[i].evaluate(tuple);

            // Type-cast the source array based on the input type that this function wants to receive.
            ATypeTag targetTypeTag = finfo.getParamList().get(i).getTypeTag();
            ATypeTag sourceTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal
                    .getStartOffset()]);
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
