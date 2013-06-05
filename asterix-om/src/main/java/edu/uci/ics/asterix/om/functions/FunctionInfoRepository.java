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
package edu.uci.ics.asterix.om.functions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionInfoRepository {
    private final Map<FunctionSignature, IFunctionInfo> functionMap;

    public FunctionInfoRepository() {
        functionMap = new ConcurrentHashMap<FunctionSignature, IFunctionInfo>();
    }

    public IFunctionInfo get(String namespace, String name, int arity) {
        FunctionSignature fname = new FunctionSignature(namespace, name, arity);
        return functionMap.get(fname);
    }

    public IFunctionInfo get(FunctionIdentifier fid) {
        return get(fid.getNamespace(), fid.getName(), fid.getArity());
    }

    public void put(String namespace, String name, int arity) {
        FunctionSignature functionSignature = new FunctionSignature(namespace, name, arity);
        functionMap.put(functionSignature, new AsterixFunctionInfo(new FunctionIdentifier(namespace, name, arity)));
    }

    public void put(FunctionIdentifier fid) {
        FunctionSignature functionSignature = new FunctionSignature(fid.getNamespace(), fid.getName(), fid.getArity());
        functionMap.put(functionSignature, new AsterixFunctionInfo(fid));
    }
}

