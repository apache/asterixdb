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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class ExternalFunctionDescriptorProvider {

    public static IFunctionDescriptor getExternalFunctionDescriptor(IExternalFunctionInfo finfo)
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