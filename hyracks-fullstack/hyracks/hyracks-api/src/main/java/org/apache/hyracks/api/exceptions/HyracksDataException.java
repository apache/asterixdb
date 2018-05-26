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

package org.apache.hyracks.api.exceptions;

import java.io.Serializable;

import org.apache.hyracks.api.util.ErrorMessageUtil;

/**
 * The main execution time exception type for runtime errors in a hyracks environment
 */
public class HyracksDataException extends HyracksException {

    private static final long serialVersionUID = 1L;

    /**
     * Wrap the failure cause in a HyracksDataException.
     * If the cause is an InterruptedException, the thread is interrupted first.
     * If the cause is already a HyracksDataException, then return it as it is.
     *
     * @param cause
     *            the root failure
     * @return the wrapped failure
     */
    public static HyracksDataException create(Throwable cause) {
        if (cause == null) {
            throw new NullPointerException("Attempt to wrap null in a HyracksDataException");
        }
        if (cause instanceof HyracksDataException) {
            return (HyracksDataException) cause;
        } else if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new HyracksDataException(cause);
    }

    public static HyracksDataException create(int code, SourceLocation sourceLoc, Serializable... params) {
        return new HyracksDataException(ErrorCode.HYRACKS, code, ErrorCode.getErrorMessage(code), null, sourceLoc,
                params);
    }

    public static HyracksDataException create(int code, Serializable... params) {
        return new HyracksDataException(ErrorCode.HYRACKS, code, ErrorCode.getErrorMessage(code), params);
    }

    public static HyracksDataException create(int code, Throwable cause, SourceLocation sourceLoc,
            Serializable... params) {
        return new HyracksDataException(ErrorCode.HYRACKS, code, ErrorCode.getErrorMessage(code), cause, sourceLoc,
                params);
    }

    public static HyracksDataException create(int code, Throwable cause, Serializable... params) {
        return new HyracksDataException(ErrorCode.HYRACKS, code, ErrorCode.getErrorMessage(code), cause, params);
    }

    public HyracksDataException(String component, int errorCode, String message, Throwable cause, String nodeId,
            Serializable... params) {
        super(component, errorCode, message, cause, nodeId, params);
    }

    public HyracksDataException(String component, int errorCode, String message, Throwable cause, String nodeId,
            StackTraceElement[] stackTrace, Serializable... params) {
        super(component, errorCode, message, cause, nodeId, params);
        setStackTrace(stackTrace);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksDataException(String message) {
        super(message);
    }

    protected HyracksDataException(Throwable cause) {
        super(cause);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public HyracksDataException(String component, int errorCode, Serializable... params) {
        super(component, errorCode, null, null, null, params);
    }

    public HyracksDataException(Throwable cause, int errorCode, Serializable... params) {
        super(ErrorMessageUtil.NONE, errorCode, cause.getMessage(), cause, null, params);
    }

    public HyracksDataException(String component, int errorCode, String message, Serializable... params) {
        super(component, errorCode, message, null, null, params);
    }

    public HyracksDataException(String component, int errorCode, Throwable cause, Serializable... params) {
        super(component, errorCode, cause, params);
    }

    public HyracksDataException(String component, int errorCode, String message, Throwable cause,
            Serializable... params) {
        super(component, errorCode, message, cause, null, params);
    }

    public HyracksDataException(String component, int errorCode, String message, Throwable cause,
            SourceLocation sourceLoc, Serializable... params) {
        super(component, errorCode, message, cause, sourceLoc, null, params);
    }

    public static HyracksDataException create(HyracksDataException e, String nodeId) {
        return new HyracksDataException(e.getComponent(), e.getErrorCode(), e.getMessage(), e.getCause(), nodeId,
                e.getStackTrace(), e.getParams());
    }
}
