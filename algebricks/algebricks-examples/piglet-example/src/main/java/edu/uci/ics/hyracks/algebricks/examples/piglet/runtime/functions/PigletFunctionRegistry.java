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
package edu.uci.ics.hyracks.algebricks.examples.piglet.runtime.functions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.examples.piglet.exceptions.PigletException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;

public class PigletFunctionRegistry {
    private static final Map<FunctionIdentifier, IPigletFunctionEvaluatorFactoryBuilder> builderMap;

    static {
        Map<FunctionIdentifier, IPigletFunctionEvaluatorFactoryBuilder> temp = new HashMap<FunctionIdentifier, IPigletFunctionEvaluatorFactoryBuilder>();

        temp.put(AlgebricksBuiltinFunctions.EQ, new IPigletFunctionEvaluatorFactoryBuilder() {
            @Override
            public ICopyEvaluatorFactory buildEvaluatorFactory(FunctionIdentifier fid, ICopyEvaluatorFactory[] arguments) {
                return new IntegerEqFunctionEvaluatorFactory(arguments[0], arguments[1]);
            }
        });

        builderMap = Collections.unmodifiableMap(temp);
    }

    public static ICopyEvaluatorFactory createFunctionEvaluatorFactory(FunctionIdentifier fid, ICopyEvaluatorFactory[] args)
            throws PigletException {
        IPigletFunctionEvaluatorFactoryBuilder builder = builderMap.get(fid);
        if (builder == null) {
            throw new PigletException("Unknown function: " + fid);
        }
        return builder.buildEvaluatorFactory(fid, args);
    }
}