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
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class ExternalFunctionDescriptorProvider {

    public static IFunctionDescriptor getExternalFunctionDescriptor(IExternalFunctionInfo finfo,
            IApplicationContext appCtx) throws AlgebricksException {
        switch (finfo.getKind()) {
            case SCALAR:
                return new ExternalScalarFunctionDescriptor(finfo, appCtx);
            case AGGREGATE:
            case UNNEST:
                throw new AlgebricksException("Unsupported function kind :" + finfo.getKind());
            default:
                break;
        }
        return null;
    }

}

class ExternalScalarFunctionDescriptor extends AbstractScalarFunctionDynamicDescriptor implements IFunctionDescriptor {
    private static final long serialVersionUID = 1L;
    private final IFunctionInfo finfo;
    private IScalarEvaluatorFactory evaluatorFactory;
    private final transient IApplicationContext appCtx;

    public ExternalScalarFunctionDescriptor(IFunctionInfo finfo, IApplicationContext appCtx) {
        this.finfo = finfo;
        this.appCtx = appCtx;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) throws AlgebricksException {
        evaluatorFactory = new ExternalScalarFunctionEvaluatorFactory((IExternalFunctionInfo) finfo, args, appCtx);
        return evaluatorFactory;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return finfo.getFunctionIdentifier();
    }

}
