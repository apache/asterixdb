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
package org.apache.asterix.common.exceptions;

import java.io.Serializable;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class AsterixException extends AlgebricksException {
    private static final long serialVersionUID = 1L;

    /**
     * @deprecated Instead, use a constructor with error code
     */
    @Deprecated
    public AsterixException(String message) {
        super(message);
    }

    public AsterixException(int errorCode, SourceLocation sourceLoc, Serializable... params) {
        super(ErrorCode.ASTERIX, errorCode, ErrorCode.getErrorMessage(errorCode), sourceLoc, params);
    }

    public AsterixException(int errorCode, Serializable... params) {
        super(ErrorCode.ASTERIX, errorCode, ErrorCode.getErrorMessage(errorCode), params);
    }

    /**
     * @deprecated When creating a constructor with cause,
     *             create AlgebricksException using AlgebricksException.create(Throwable th);
     */
    @Deprecated
    public AsterixException(Throwable cause) {
        super(cause);
    }

    public AsterixException(int errorCode, Throwable cause, SourceLocation sourceLoc, Serializable... params) {
        super(ErrorCode.ASTERIX, errorCode, ErrorCode.getErrorMessage(errorCode), sourceLoc, params);
        addSuppressed(cause);
    }

    public AsterixException(int errorCode, Throwable cause, Serializable... params) {
        super(ErrorCode.ASTERIX, errorCode, ErrorCode.getErrorMessage(errorCode), params);
        addSuppressed(cause);
    }

    /**
     * @deprecated Instead, use a constructor with error code
     */
    @Deprecated
    public AsterixException(String message, Throwable cause) {
        super(message, cause);
    }

    public static AsterixException create(int errorCode, SourceLocation sourceLoc, Serializable... params) {
        return new AsterixException(errorCode, sourceLoc, params);
    }

    public static AsterixException create(int errorCode, Serializable... params) {
        return new AsterixException(errorCode, params);
    }
}
