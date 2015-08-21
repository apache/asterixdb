/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package org.apache.asterix.metadata.functions;

import java.util.List;

import org.apache.asterix.om.functions.AsterixExternalFunctionInfo;
import org.apache.asterix.om.functions.AsterixFunction;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;

public class AsterixExternalScalarFunctionInfo extends AsterixExternalFunctionInfo {

    private static final long serialVersionUID = 1L;

    public AsterixExternalScalarFunctionInfo(String namespace, AsterixFunction asterixFunction, IAType returnType,
            String body, String language, List<IAType> argumentTypes, IResultTypeComputer rtc) {
        super(namespace, asterixFunction, FunctionKind.SCALAR, argumentTypes, returnType, rtc, body, language);
    }

    public AsterixExternalScalarFunctionInfo() {
        super();
    }

}
