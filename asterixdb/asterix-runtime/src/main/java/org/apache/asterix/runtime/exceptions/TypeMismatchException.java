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

package org.apache.asterix.runtime.exceptions;

import static org.apache.asterix.om.exceptions.ExceptionUtil.indexToPosition;
import static org.apache.asterix.om.exceptions.ExceptionUtil.toExpectedTypeString;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class TypeMismatchException extends RuntimeDataException {
    private static final long serialVersionUID = -668005043013338591L;

    // Function parameter type mismatch.
    public TypeMismatchException(FunctionIdentifier fid, Integer i, byte actualTypeTag, byte... expectedTypeTags) {
        super(ErrorCode.TYPE_MISMATCH_FUNCTION, fid.getName(), indexToPosition(i),
                toExpectedTypeString(expectedTypeTags),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(actualTypeTag));
    }

    // Function parameter type mismatch.
    public TypeMismatchException(SourceLocation sourceLoc, FunctionIdentifier fid, Integer i, byte actualTypeTag,
            byte... expectedTypeTags) {
        super(ErrorCode.TYPE_MISMATCH_FUNCTION, sourceLoc, fid.getName(), indexToPosition(i),
                toExpectedTypeString(expectedTypeTags),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(actualTypeTag));
    }

    // Function parameter type mismatch.
    public TypeMismatchException(SourceLocation sourceLoc, FunctionIdentifier fid, Integer i, byte actualTypeTag,
            String expectedType) {
        super(ErrorCode.TYPE_MISMATCH_FUNCTION, sourceLoc, fid.getName(), indexToPosition(i), expectedType,
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(actualTypeTag));
    }

    // Function parameter type mismatch.
    @Deprecated
    public TypeMismatchException(String functionName, Integer i, byte actualTypeTag, byte... expectedTypeTags) {
        super(ErrorCode.TYPE_MISMATCH_FUNCTION, functionName, indexToPosition(i),
                toExpectedTypeString(expectedTypeTags),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(actualTypeTag));
    }

    // Function parameter type mismatch.
    @Deprecated
    public TypeMismatchException(SourceLocation sourceLoc, String functionName, Integer i, byte actualTypeTag,
            byte... expectedTypeTags) {
        super(ErrorCode.TYPE_MISMATCH_FUNCTION, sourceLoc, functionName, indexToPosition(i),
                toExpectedTypeString(expectedTypeTags),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(actualTypeTag));
    }

    // Generic type mismatch.
    public TypeMismatchException(SourceLocation sourceLoc, byte actualTypeTag, byte... expectedTypeTags) {
        super(ErrorCode.TYPE_MISMATCH_GENERIC, sourceLoc, toExpectedTypeString(expectedTypeTags),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(actualTypeTag));
    }

    // Generic type mismatch.
    public TypeMismatchException(SourceLocation sourceLoc, byte actualTypeTag, String expectedType) {
        super(ErrorCode.TYPE_MISMATCH_GENERIC, sourceLoc, expectedType,
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(actualTypeTag));
    }
}
