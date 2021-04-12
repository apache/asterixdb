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
package org.apache.asterix.external.library;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.functions.IExternalFunctionDescriptor;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;

public class ExternalFunctionDescriptorProvider {

    public static IExternalFunctionDescriptor resolveExternalFunction(AbstractFunctionCallExpression expr,
            IVariableTypeEnvironment inputTypeEnv, JobGenContext context) throws AlgebricksException {
        IExternalFunctionDescriptor fd = getExternalFunctionDescriptor((IExternalFunctionInfo) expr.getFunctionInfo());
        CompilerProperties props = ((IApplicationContext) context.getAppContext()).getCompilerProperties();
        FunctionTypeInferers.SET_ARGUMENTS_TYPE.infer(expr, fd, inputTypeEnv, props);
        fd.setSourceLocation(expr.getSourceLocation());
        return fd;
    }

    private static IExternalFunctionDescriptor getExternalFunctionDescriptor(IExternalFunctionInfo finfo)
            throws AlgebricksException {
        switch (finfo.getKind()) {
            case SCALAR:
                return new ExternalScalarFunctionDescriptor(finfo);
            case AGGREGATE:
            case UNNEST:
                throw new AsterixException(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND, finfo.getKind());
            default:
                throw new AsterixException(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNKNOWN_KIND, finfo.getKind());
        }
    }
}
