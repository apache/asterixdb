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
package edu.uci.ics.asterix.aql.context;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.functions.FunctionSignature;

public class FunctionSignatures {
    private final Map<FunctionSignature, FunctionExpressionMap> functionMap;

    public FunctionSignatures() {
        functionMap = new HashMap<FunctionSignature, FunctionExpressionMap>();
    }

    public FunctionSignature get(String dataverse, String name, int arity) {
        FunctionSignature fid = new FunctionSignature(dataverse, name, arity);
        FunctionExpressionMap possibleFD = functionMap.get(fid);
        if (possibleFD == null) {
            return null;
        } else {
            return possibleFD.get(arity);
        }
    }

    public void put(FunctionSignature fd, boolean varargs) {
        FunctionExpressionMap func = functionMap.get(fd);
        if (func == null) {
            func = new FunctionExpressionMap(varargs);
            functionMap.put(fd, func);
        }
        func.put(fd.getArity(), fd);
    }
}
