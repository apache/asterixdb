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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class UnsupportedTypeException extends CompilationException {
    private static final long serialVersionUID = -641723317638101625L;

    // Unsupported input type.
    public UnsupportedTypeException(FunctionIdentifier fid, ATypeTag actualTypeTag) {
        super(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, fid.getName(), actualTypeTag);
    }

    // Unsupported input type.
    public UnsupportedTypeException(SourceLocation sourceLoc, FunctionIdentifier fid, ATypeTag actualTypeTag) {
        super(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, sourceLoc, fid.getName(), actualTypeTag);
    }

    // Unsupported input type.
    @Deprecated
    public UnsupportedTypeException(String funcName, ATypeTag actualTypeTag) {
        super(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, funcName, actualTypeTag);
    }

    // Unsupported input type.
    @Deprecated
    public UnsupportedTypeException(SourceLocation sourceLoc, String funcName, ATypeTag actualTypeTag) {
        super(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, sourceLoc, funcName, actualTypeTag);
    }
}
