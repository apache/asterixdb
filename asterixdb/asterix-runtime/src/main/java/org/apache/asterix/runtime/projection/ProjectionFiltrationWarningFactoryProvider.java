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
package org.apache.asterix.runtime.projection;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;

public class ProjectionFiltrationWarningFactoryProvider {
    private ProjectionFiltrationWarningFactoryProvider() {
    }

    public static final IProjectionFiltrationWarningFactory TYPE_MISMATCH_FACTORY =
            new IProjectionFiltrationWarningFactory() {
                private static final long serialVersionUID = 4263556611813387010L;

                @Override
                public Warning createWarning(SourceLocation sourceLocation, String functionName, String position,
                        ATypeTag expectedType, ATypeTag actualType) {
                    return Warning.of(sourceLocation, ErrorCode.TYPE_MISMATCH_FUNCTION, functionName,
                            ExceptionUtil.indexToPosition(0), expectedType, actualType);
                }

                @Override
                public ErrorCode getErrorCode() {
                    return ErrorCode.TYPE_MISMATCH_FUNCTION;
                }
            };

    public static IProjectionFiltrationWarningFactory getIncomparableTypesFactory(boolean leftConstant) {
        return leftConstant ? LEFT_CONSTANT_INCOMPARABLE_TYPES_FACTORY : RIGHT_CONSTANT_INCOMPARABLE_TYPES_FACTORY;
    }

    private static final IProjectionFiltrationWarningFactory LEFT_CONSTANT_INCOMPARABLE_TYPES_FACTORY =
            new IProjectionFiltrationWarningFactory() {
                private static final long serialVersionUID = -7447187099851545763L;

                @Override
                public Warning createWarning(SourceLocation sourceLocation, String functionName, String position,
                        ATypeTag expectedType, ATypeTag actualType) {
                    return Warning.of(sourceLocation, ErrorCode.INCOMPARABLE_TYPES, expectedType, actualType);
                }

                @Override
                public ErrorCode getErrorCode() {
                    return ErrorCode.INCOMPARABLE_TYPES;
                }
            };

    private static final IProjectionFiltrationWarningFactory RIGHT_CONSTANT_INCOMPARABLE_TYPES_FACTORY =
            new IProjectionFiltrationWarningFactory() {
                private static final long serialVersionUID = 2818081955008928378L;

                @Override
                public Warning createWarning(SourceLocation sourceLocation, String functionName, String position,
                        ATypeTag expectedType, ATypeTag actualType) {
                    return Warning.of(sourceLocation, ErrorCode.INCOMPARABLE_TYPES, actualType, expectedType);
                }

                @Override
                public ErrorCode getErrorCode() {
                    return ErrorCode.INCOMPARABLE_TYPES;
                }
            };
}
