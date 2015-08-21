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
package org.apache.asterix.external.library;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;

public class ExternalFunctionDescriptorProvider {

    public static IFunctionDescriptor getExternalFunctionDescriptor(IExternalFunctionInfo finfo)
            throws AsterixException {
        switch (finfo.getKind()) {
            case SCALAR:
                return new ExternalScalarFunctionDescriptor(finfo);
            case AGGREGATE:
            case UNNEST:
                throw new AsterixException("Unsupported function kind :" + finfo.getKind());
            default:
                break;
        }
        return null;
    }

}

class ExternalScalarFunctionDescriptor extends AbstractScalarFunctionDynamicDescriptor implements IFunctionDescriptor {

    private final IFunctionInfo finfo;
    private ICopyEvaluatorFactory evaluatorFactory;
    private ICopyEvaluatorFactory[] args;

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(ICopyEvaluatorFactory[] args) throws AlgebricksException {
        evaluatorFactory = new ExternalScalarFunctionEvaluatorFactory((IExternalFunctionInfo) finfo, args);
        return evaluatorFactory;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return finfo.getFunctionIdentifier();
    }

    public ExternalScalarFunctionDescriptor(IFunctionInfo finfo) {
        this.finfo = finfo;
    }

}
