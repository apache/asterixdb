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

package org.apache.asterix.runtime.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Default implementation of {@link IFunctionManager}.
 */
public final class FunctionManager implements IFunctionManager {

    private final Map<Pair<FunctionIdentifier, Integer>, IFunctionDescriptorFactory> functions;

    private final Map<FunctionIdentifier, IFunctionTypeInferer> typeInferers;

    public FunctionManager(FunctionCollection functionCollection) {
        Map<Pair<FunctionIdentifier, Integer>, IFunctionDescriptorFactory> functionsMap = new HashMap<>();
        Map<FunctionIdentifier, IFunctionTypeInferer> typeInferersMap = new HashMap<>();

        for (IFunctionDescriptorFactory descriptorFactory : functionCollection.getFunctionDescriptorFactories()) {
            FunctionIdentifier fid = descriptorFactory.createFunctionDescriptor().getIdentifier();
            functionsMap.put(new Pair<>(fid, fid.getArity()), descriptorFactory);
            IFunctionTypeInferer typeInferer = descriptorFactory.createFunctionTypeInferer();
            if (typeInferer != null) {
                typeInferersMap.put(fid, typeInferer);
            }
        }

        this.functions = functionsMap;
        this.typeInferers = typeInferersMap;
    }

    @Override
    public IFunctionDescriptor lookupFunction(FunctionIdentifier fid, SourceLocation src) throws AlgebricksException {
        Pair<FunctionIdentifier, Integer> key = new Pair<>(fid, fid.getArity());
        IFunctionDescriptorFactory factory = functions.get(key);
        if (factory == null) {
            String msg = "Inappropriate use of function '" + fid.getName() + "'";
            if (fid.equals(BuiltinFunctions.META)) {
                msg = msg + ". For example, after GROUP BY";
            }
            throw AsterixException.create(ErrorCode.COMPILATION_ERROR, src, msg);
        }
        return factory.createFunctionDescriptor();
    }

    @Override
    public IFunctionTypeInferer lookupFunctionTypeInferer(FunctionIdentifier fid) {
        return typeInferers.get(fid);
    }
}
