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

import static org.apache.asterix.runtime.exceptions.ExceptionUtil.indexToPosition;
import static org.apache.asterix.runtime.exceptions.ExceptionUtil.toExpectedTypeString;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class TypeMismatchException extends RuntimeDataException {

    // Parameter type mistmatch.
    public TypeMismatchException(FunctionIdentifier fid, Integer i, byte actualTypeTag, byte... expectedTypeTags) {
        super(ErrorCode.ERROR_TYPE_MISMATCH, fid.getName(), indexToPosition(i), toExpectedTypeString(expectedTypeTags),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(actualTypeTag));
    }

    // Parameter type mistmatch.
    public TypeMismatchException(String functionName, Integer i, byte actualTypeTag, byte... expectedTypeTags) {
        super(ErrorCode.ERROR_TYPE_MISMATCH, functionName, indexToPosition(i), toExpectedTypeString(expectedTypeTags),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(actualTypeTag));
    }


}
