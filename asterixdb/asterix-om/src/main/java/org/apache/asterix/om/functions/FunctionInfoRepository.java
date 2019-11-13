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
package org.apache.asterix.om.functions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionInfoRepository {
    private final Map<FunctionSignature, IFunctionInfo> functionMap;

    public FunctionInfoRepository() {
        functionMap = new ConcurrentHashMap<>();
    }

    private IFunctionInfo get(FunctionSignature functionSignature) {
        return functionMap.get(functionSignature);
    }

    private void put(FunctionSignature functionSignature, IFunctionInfo fInfo) {
        functionMap.put(functionSignature, fInfo);
    }

    public IFunctionInfo get(FunctionIdentifier fid) {
        return get(new FunctionSignature(fid));
    }

    public void put(FunctionIdentifier fid, IFunctionInfo fInfo) {
        put(new FunctionSignature(fid), fInfo);
    }
}
