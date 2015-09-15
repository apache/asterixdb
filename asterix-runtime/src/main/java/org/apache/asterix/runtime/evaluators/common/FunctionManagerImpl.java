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

package org.apache.asterix.runtime.evaluators.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FunctionManagerImpl implements IFunctionManager {
    private final Map<Pair<FunctionIdentifier, Integer>, IFunctionDescriptorFactory> functions;

    public FunctionManagerImpl() {
        functions = new HashMap<Pair<FunctionIdentifier, Integer>, IFunctionDescriptorFactory>();
    }

    @Override
    public synchronized IFunctionDescriptor lookupFunction(FunctionIdentifier fid) throws AlgebricksException {
        Pair<FunctionIdentifier, Integer> key = new Pair<FunctionIdentifier, Integer>(fid, fid.getArity());
        IFunctionDescriptorFactory factory = functions.get(key);
        if (factory == null) {
            throw new AlgebricksException("Inappropriate use of function " + "'" + fid.getName() + "'");
        }
        return factory.createFunctionDescriptor();
    }

    @Override
    public synchronized void registerFunction(IFunctionDescriptorFactory descriptorFactory) throws AlgebricksException {
        FunctionIdentifier fid = descriptorFactory.createFunctionDescriptor().getIdentifier();
        functions.put(new Pair<FunctionIdentifier, Integer>(fid, fid.getArity()), descriptorFactory);
    }

    @Override
    public synchronized void unregisterFunction(IFunctionDescriptorFactory descriptorFactory)
            throws AlgebricksException {
        FunctionIdentifier fid = descriptorFactory.createFunctionDescriptor().getIdentifier();
        Pair<FunctionIdentifier, Integer> key = new Pair<FunctionIdentifier, Integer>(fid, fid.getArity());
        functions.remove(key);
    }

    @Override
    public synchronized Iterator<IFunctionDescriptorFactory> iterator() {
        return new ArrayList<IFunctionDescriptorFactory>(functions.values()).iterator();
    }
}