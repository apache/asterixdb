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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class InvalidDataFormatException extends RuntimeDataException {
    private static final long serialVersionUID = 7927137063741221011L;

    public InvalidDataFormatException(SourceLocation sourceLoc, FunctionIdentifier fid, byte expectedTypeTag) {
        super(ErrorCode.INVALID_FORMAT, sourceLoc, fid.getName(),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(expectedTypeTag));
    }

    public InvalidDataFormatException(SourceLocation sourceLoc, FunctionIdentifier fid, String expectedType) {
        super(ErrorCode.INVALID_FORMAT, sourceLoc, fid.getName(), expectedType);
    }

    public InvalidDataFormatException(SourceLocation sourceLoc, FunctionIdentifier fid, Throwable cause,
            byte expectedTypeTag) {
        super(ErrorCode.INVALID_FORMAT, sourceLoc, fid.getName(), cause, expectedTypeTag);
        addSuppressed(cause);
    }

}
