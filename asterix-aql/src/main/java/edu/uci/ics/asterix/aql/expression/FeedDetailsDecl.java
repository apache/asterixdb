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
package edu.uci.ics.asterix.aql.expression;

import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.common.functions.FunctionSignature;

public class FeedDetailsDecl extends InternalDetailsDecl {
    private final Map<String, String> configuration;
    private final String adapterFactoryClassname;
    private final FunctionSignature functionSignature;

    public FeedDetailsDecl(String adapterFactoryClassname, Map<String, String> configuration,
            FunctionSignature signature, Identifier nodeGroupName, List<String> partitioningExpr,
            String compactionPolicy, Map<String, String> compactionPolicyProperties) {
        super(nodeGroupName, partitioningExpr, compactionPolicy, compactionPolicyProperties);
        this.adapterFactoryClassname = adapterFactoryClassname;
        this.configuration = configuration;
        this.functionSignature = signature;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public String getAdapterFactoryClassname() {
        return adapterFactoryClassname;
    }

    public FunctionSignature getSignature() {
        return functionSignature;
    }

    public FunctionSignature getFunctionSignature() {
        return functionSignature;
    }
}
