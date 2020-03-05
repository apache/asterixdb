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
package org.apache.asterix.metadata.functions;

import java.util.List;
import java.util.Map;

import org.apache.asterix.om.functions.ExternalFunctionInfo;
import org.apache.asterix.om.functions.ExternalFunctionLanguage;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class ExternalScalarFunctionInfo extends ExternalFunctionInfo {

    private static final long serialVersionUID = 2L;

    public ExternalScalarFunctionInfo(String namespace, String library, String name, int arity, IAType returnType,
            List<String> externalIdentifier, ExternalFunctionLanguage language, List<IAType> argumentTypes,
            Map<String, String> params, boolean deterministic, IResultTypeComputer rtc) {
        super(namespace, name, arity, FunctionKind.SCALAR, argumentTypes, returnType, rtc, language, library,
                externalIdentifier, params, deterministic);
    }

    public ExternalScalarFunctionInfo(FunctionIdentifier fid, IAType returnType, List<String> externalIdentifier,
            ExternalFunctionLanguage language, String library, List<IAType> argumentTypes, Map<String, String> params,
            boolean deterministic, IResultTypeComputer rtc) {
        super(fid, FunctionKind.SCALAR, argumentTypes, returnType, rtc, language, library, externalIdentifier, params,
                deterministic);
    }
}
