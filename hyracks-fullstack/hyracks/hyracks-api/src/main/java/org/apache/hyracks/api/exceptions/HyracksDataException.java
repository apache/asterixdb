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

    public static HyracksDataException create(ErrorCode code, SourceLocation sourceLoc, Serializable... params) {
        return new HyracksDataException(code, sourceLoc, params);
    }

    public static HyracksDataException create(ErrorCode code, Serializable... params) {
        return new HyracksDataException(code, params);
    }

    public static HyracksDataException create(ErrorCode code, Throwable cause, SourceLocation sourceLoc,
            Serializable... params) {
        return new HyracksDataException(code, cause, sourceLoc, params);
    }

    public static HyracksDataException create(ErrorCode code, Throwable cause, Serializable... params) {
        return new HyracksDataException(code, cause, null, params);
    }

    public static HyracksDataException create(HyracksDataException e, String nodeId) {
        return new HyracksDataException(e, nodeId);
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

    public HyracksDataException(ErrorCode code, Serializable... params) {
        this(code, null, params);
    }

    public HyracksDataException(ErrorCode code, SourceLocation sourceLoc, Serializable... params) {
        this(code, null, sourceLoc, params);
    }

    public HyracksDataException(ErrorCode code, Throwable cause, SourceLocation sourceLoc, Serializable... params) {
        super(code, code.component(), code.intValue(), code.errorMessage(), cause, sourceLoc, null, params);
    }

    private HyracksDataException(HyracksDataException hde, String nodeId) {
        super(hde.getError().orElse(null), hde.getComponent(), hde.getErrorCode(), hde.getMessage(), hde.getCause(),
                hde.getSourceLocation(), nodeId, hde.getParams());
    }

    protected HyracksDataException(IError error, Throwable cause, SourceLocation sourceLoc, Serializable... params) {
        super(error, error.component(), error.intValue(), error.errorMessage(), cause, sourceLoc, null, params);
    }
}
