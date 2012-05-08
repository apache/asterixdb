package edu.uci.ics.asterix.optimizer.base;


import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FuzzyUtils {

    private final static String DEFAULT_SIM_FUNCTION = "jaccard";
    private final static float DEFAULT_SIM_THRESHOLD = .8f;

    private final static String SIM_FUNCTION_PROP_NAME = "simfunction";
    private final static String SIM_THRESHOLD_PROP_NAME = "simthreshold";

    public static FunctionIdentifier getTokenizer(ATypeTag inputTag) {
        switch (inputTag) {
            case STRING:
                return AsterixBuiltinFunctions.COUNTHASHED_WORD_TOKENS;
            case UNORDEREDLIST:
            case ORDEREDLIST:
                return null;
            default:
                throw new NotImplementedException("No tokenizer for type " + inputTag);
        }
    }

    public static float getSimThreshold(AqlCompiledMetadataDeclarations metadata) {
        float simThreshold = DEFAULT_SIM_THRESHOLD;
        String simThresholValue = metadata.getPropertyValue(SIM_THRESHOLD_PROP_NAME);
        if (simThresholValue != null) {
            simThreshold = Float.parseFloat(simThresholValue);
        }
        return simThreshold;
    }

    public static String getSimFunction(AqlCompiledMetadataDeclarations metadata) {
        String simFunction = metadata.getPropertyValue(SIM_FUNCTION_PROP_NAME);
        if (simFunction == null) {
            simFunction = DEFAULT_SIM_FUNCTION;
        }
        simFunction = simFunction.toLowerCase();
        return simFunction;
    }
}
