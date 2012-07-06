package edu.uci.ics.asterix.optimizer.base;

import java.util.ArrayList;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FuzzyUtils {

    private final static String DEFAULT_SIM_FUNCTION = "jaccard";
    private final static float JACCARD_DEFAULT_SIM_THRESHOLD = .8f;
    private final static int EDIT_DISTANCE_DEFAULT_SIM_THRESHOLD = 1;

    private final static String SIM_FUNCTION_PROP_NAME = "simfunction";
    private final static String SIM_THRESHOLD_PROP_NAME = "simthreshold";

    public final static String JACCARD_FUNCTION_NAME = "jaccard";
    public final static String EDIT_DISTANCE_FUNCTION_NAME = "edit-distance";

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

    public static IAObject getSimThreshold(AqlCompiledMetadataDeclarations metadata, String simFuncName) {
        String simThresholValue = metadata.getPropertyValue(SIM_THRESHOLD_PROP_NAME);
        IAObject ret = null;
        if (simFuncName.equals(JACCARD_FUNCTION_NAME)) {
            if (simThresholValue != null) {
                float jaccThresh = Float.parseFloat(simThresholValue);
                ret = new AFloat(jaccThresh);
            } else {
                ret = new AFloat(JACCARD_DEFAULT_SIM_THRESHOLD);
            }
        } else if (simFuncName.equals(EDIT_DISTANCE_FUNCTION_NAME)) {
            if (simThresholValue != null) {
                int edThresh = Integer.parseInt(simThresholValue);
                ret = new AInt32(edThresh);
            } else {
                ret = new AFloat(EDIT_DISTANCE_DEFAULT_SIM_THRESHOLD);
            }
        }
        return ret;
    }

    public static FunctionIdentifier getFunctionIdentifier(String simFuncName) {
        if (simFuncName.equals(JACCARD_FUNCTION_NAME)) {
            return AsterixBuiltinFunctions.SIMILARITY_JACCARD;
        } else if (simFuncName.equals(EDIT_DISTANCE_FUNCTION_NAME)) {
            return AsterixBuiltinFunctions.EDIT_DISTANCE;
        }
        return null;
    }

    public static ScalarFunctionCallExpression getComparisonExpr(String simFuncName,
            ArrayList<Mutable<ILogicalExpression>> cmpArgs) {
        if (simFuncName.equals(JACCARD_FUNCTION_NAME)) {
            return new ScalarFunctionCallExpression(FunctionUtils.getFunctionInfo(AlgebricksBuiltinFunctions.GE),
                    cmpArgs);
        } else if (simFuncName.equals(EDIT_DISTANCE_FUNCTION_NAME)) {
            return new ScalarFunctionCallExpression(FunctionUtils.getFunctionInfo(AlgebricksBuiltinFunctions.LE),
                    cmpArgs);
        }
        return null;
    }

    public static float getSimThreshold(AqlCompiledMetadataDeclarations metadata) {
        float simThreshold = JACCARD_DEFAULT_SIM_THRESHOLD;
        String simThresholValue = metadata.getPropertyValue(SIM_THRESHOLD_PROP_NAME);
        if (simThresholValue != null) {
            simThreshold = Float.parseFloat(simThresholValue);
        }
        return simThreshold;
    }

    // TODO: The default function depend on the input types. 
    public static String getSimFunction(AqlCompiledMetadataDeclarations metadata) {
        String simFunction = metadata.getPropertyValue(SIM_FUNCTION_PROP_NAME);
        if (simFunction == null) {
            simFunction = DEFAULT_SIM_FUNCTION;
        }
        simFunction = simFunction.toLowerCase();
        return simFunction;
    }
}
