package edu.uci.ics.asterix.external.library;

import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
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

        String[] fnameComponents = finfo.getFunctionIdentifier().getName().split(":");
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
            functionHelper.setArgument(i, inputVal.getByteArray());
        }
    }

    @Override
    public void deinitialize() {
        externalFunction.deinitialize();
    }

}
