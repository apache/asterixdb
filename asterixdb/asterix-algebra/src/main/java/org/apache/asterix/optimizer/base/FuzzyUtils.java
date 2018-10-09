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
package org.apache.asterix.optimizer.base;

import java.util.ArrayList;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FuzzyUtils {

    private final static String DEFAULT_SIM_FUNCTION = "jaccard";
    private final static float JACCARD_DEFAULT_SIM_THRESHOLD = .8f;
    private final static int EDIT_DISTANCE_DEFAULT_SIM_THRESHOLD = 1;

    public final static String SIM_FUNCTION_PROP_NAME = "simfunction";
    public final static String SIM_THRESHOLD_PROP_NAME = "simthreshold";

    public final static String JACCARD_FUNCTION_NAME = "jaccard";
    public final static String EDIT_DISTANCE_FUNCTION_NAME = "edit-distance";

    public static FunctionIdentifier getTokenizer(ATypeTag inputTag) {
        switch (inputTag) {
            case STRING:
                return BuiltinFunctions.COUNTHASHED_WORD_TOKENS;
            case MULTISET:
            case ARRAY:
            case UNION:
            case ANY:
                return null;
            default:
                throw new NotImplementedException("No tokenizer for type " + inputTag);
        }
    }

    public static IAObject getSimThreshold(MetadataProvider metadata, String simFuncName) {
        String simThresholValue = metadata.getProperty(SIM_THRESHOLD_PROP_NAME);
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
            return BuiltinFunctions.SIMILARITY_JACCARD;
        } else if (simFuncName.equals(EDIT_DISTANCE_FUNCTION_NAME)) {
            return BuiltinFunctions.EDIT_DISTANCE;
        }
        return null;
    }

    public static ScalarFunctionCallExpression getComparisonExpr(String simFuncName,
            ArrayList<Mutable<ILogicalExpression>> cmpArgs) {
        if (simFuncName.equals(JACCARD_FUNCTION_NAME)) {
            return new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.GE),
                    cmpArgs);
        } else if (simFuncName.equals(EDIT_DISTANCE_FUNCTION_NAME)) {
            return new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.LE),
                    cmpArgs);
        }
        return null;
    }

    public static float getSimThreshold(MetadataProvider metadata) {
        float simThreshold = JACCARD_DEFAULT_SIM_THRESHOLD;
        String simThresholValue = metadata.getProperty(SIM_THRESHOLD_PROP_NAME);
        if (simThresholValue != null) {
            simThreshold = Float.parseFloat(simThresholValue);
        }
        return simThreshold;
    }

    // TODO: The default function depend on the input types.
    public static String getSimFunction(MetadataProvider metadata) {
        String simFunction = metadata.getProperty(SIM_FUNCTION_PROP_NAME);
        if (simFunction == null) {
            simFunction = DEFAULT_SIM_FUNCTION;
        }
        simFunction = simFunction.toLowerCase();
        return simFunction;
    }

    public static String getSimFunction(FunctionIdentifier simFuncId) {
        if (simFuncId.equals(BuiltinFunctions.SIMILARITY_JACCARD)
                || simFuncId.equals(BuiltinFunctions.SIMILARITY_JACCARD_CHECK)) {
            return JACCARD_FUNCTION_NAME;
        } else if (simFuncId.equals(BuiltinFunctions.EDIT_DISTANCE)
                || simFuncId.equals(BuiltinFunctions.EDIT_DISTANCE_CHECK)) {
            return EDIT_DISTANCE_FUNCTION_NAME;
        }
        return null;
    }
}
