/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.asterix.om.exceptions;

import static org.apache.asterix.om.exceptions.ExceptionUtil.indexToPosition;
import static org.apache.asterix.om.exceptions.ExceptionUtil.toExpectedTypeString;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class TypeMismatchException extends CompilationException {
    private static final long serialVersionUID = -3069967719104299912L;

    // Function parameter type mismatch.
    public TypeMismatchException(FunctionIdentifier fid, Integer i, ATypeTag actualTypeTag,
            ATypeTag... expectedTypeTags) {
        super(ErrorCode.COMPILATION_TYPE_MISMATCH_FUNCTION, fid.getName(), indexToPosition(i),
                toExpectedTypeString(expectedTypeTags), actualTypeTag);
    }

    // Function parameter type mismatch.
    public TypeMismatchException(SourceLocation sourceLoc, FunctionIdentifier fid, Integer i, ATypeTag actualTypeTag,
            ATypeTag... expectedTypeTags) {
        super(ErrorCode.COMPILATION_TYPE_MISMATCH_FUNCTION, sourceLoc, fid.getName(), indexToPosition(i),
                toExpectedTypeString(expectedTypeTags), actualTypeTag);
    }

    // Function parameter type mismatch.
    @Deprecated
    public TypeMismatchException(String functionName, Integer i, ATypeTag actualTypeTag, ATypeTag... expectedTypeTags) {
        super(ErrorCode.COMPILATION_TYPE_MISMATCH_FUNCTION, functionName, indexToPosition(i),
                toExpectedTypeString(expectedTypeTags), actualTypeTag);
    }

    // Function parameter type mismatch.
    @Deprecated
    public TypeMismatchException(SourceLocation sourceLoc, String functionName, Integer i, ATypeTag actualTypeTag,
            ATypeTag... expectedTypeTags) {
        super(ErrorCode.COMPILATION_TYPE_MISMATCH_FUNCTION, sourceLoc, functionName, indexToPosition(i),
                toExpectedTypeString(expectedTypeTags), actualTypeTag);
    }

    // Generic type mismatch.
    public TypeMismatchException(SourceLocation sourceLoc, ATypeTag actualTypeTag, ATypeTag... expectedTypeTags) {
        super(ErrorCode.COMPILATION_TYPE_MISMATCH_GENERIC, sourceLoc, toExpectedTypeString(expectedTypeTags),
                actualTypeTag);
    }

    // Generic type mismatch.
    public TypeMismatchException(SourceLocation sourceLoc, ATypeTag actualTypeTag, String expectedType) {
        super(ErrorCode.COMPILATION_TYPE_MISMATCH_GENERIC, sourceLoc, expectedType, actualTypeTag);
    }
}
