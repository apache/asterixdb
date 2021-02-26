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

package org.apache.asterix.common.exceptions;

import java.io.Serializable;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class RuntimeDataException extends HyracksDataException {
    private static final long serialVersionUID = 1L;

    public static RuntimeDataException create(ErrorCode error, Serializable... params) {
        return new RuntimeDataException(error, params);
    }

    public RuntimeDataException(ErrorCode errorCode, Throwable cause, SourceLocation sourceLoc,
            Serializable... params) {
        super(errorCode, cause, sourceLoc, params);
    }

    public RuntimeDataException(ErrorCode errorCode, Serializable... params) {
        this(errorCode, null, null, params);
    }

    public RuntimeDataException(ErrorCode errorCode, SourceLocation sourceLoc, Serializable... params) {
        this(errorCode, null, sourceLoc, params);
    }

    public RuntimeDataException(ErrorCode errorCode, Throwable cause, Serializable... params) {
        this(errorCode, cause, null, params);
    }

}
