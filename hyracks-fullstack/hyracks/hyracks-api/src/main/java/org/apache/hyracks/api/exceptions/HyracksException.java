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

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

import org.apache.hyracks.api.util.ErrorMessageUtil;

public class HyracksException extends IOException implements IFormattedException {
    private static final long serialVersionUID = 2L;

    public static final int UNKNOWN = 0;
    private final String component;
    private final int errorCode;
    private final Serializable[] params;
    private final String nodeId;
    private SourceLocation sourceLoc;
    protected transient IError error;
    private transient volatile String msgCache;

    public static HyracksException create(Throwable cause) {
        if (cause instanceof HyracksException) {
            return (HyracksException) cause;
        }
        return new HyracksException(cause);
    }

    public static HyracksException wrapOrThrowUnchecked(Throwable cause) {
        if (cause instanceof Error) {
            throw (Error) cause;
        } else if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        } else if (cause instanceof HyracksException) {
            return (HyracksException) cause;
        }
        return new HyracksException(cause);
    }

    public static HyracksException create(ErrorCode code, Serializable... params) {
        return new HyracksException(code, params);
    }

    public static HyracksException create(ErrorCode code, Throwable cause, Serializable... params) {
        return new HyracksException(code, cause, params);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksException(String message) {
        this(message, null);
    }

    protected HyracksException(IError error, String component, int intCode, String message, Throwable cause,
            SourceLocation sourceLoc, String nodeId, Serializable... params) {
        super(message, cause);
        this.error = error;
        this.sourceLoc = sourceLoc;
        this.component = component;
        this.errorCode = intCode;
        this.nodeId = nodeId;
        this.params = params;
    }

    protected HyracksException(IError errorCode, Throwable cause, SourceLocation sourceLoc, String nodeId,
            Serializable... params) {
        this(errorCode, errorCode.component(), errorCode.intValue(), errorCode.errorMessage(), cause, sourceLoc, nodeId,
                params);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    protected HyracksException(Throwable cause) {
        this(String.valueOf(cause), cause);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    protected HyracksException(String message, Throwable cause) {
        this(ErrorMessageUtil.NONE, UNKNOWN, message, cause);
    }

    public HyracksException(ErrorCode errorCode, Throwable cause, Serializable... params) {
        this(errorCode.component(), errorCode.intValue(), errorCode.errorMessage(), cause, null, params);
    }

    public HyracksException(ErrorCode errorCode, Serializable... params) {
        this(errorCode.component(), errorCode.intValue(), errorCode.errorMessage(), null, null, params);
    }

    private HyracksException(String component, int errorCode, String message, Throwable cause, Serializable... params) {
        this(component, errorCode, message, cause, null, params);
    }

    private HyracksException(String component, int errorCode, String message, Throwable cause, String nodeId,
            Serializable... params) {
        this(null, component, errorCode, message, cause, null, nodeId, params);
    }

    @Override
    public String getComponent() {
        return component;
    }

    @Override
    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public Serializable[] getParams() {
        return params;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public SourceLocation getSourceLocation() {
        return sourceLoc;
    }

    public void setSourceLocation(SourceLocation sourceLocation) {
        this.sourceLoc = sourceLocation;
    }

    @Override
    public String getMessage() {
        String message = msgCache;
        if (message == null) {
            msgCache = message =
                    ErrorMessageUtil.formatMessage(component, errorCode, super.getMessage(), sourceLoc, params);
        }
        return message;
    }

    public String getMessageNoCode() {
        return ErrorMessageUtil.getMessageNoCode(component, getMessage());
    }

    @Override
    public String toString() {
        return getLocalizedMessage();
    }

    @Override
    public Optional<IError> getError() {
        return Optional.ofNullable(error);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        ErrorMessageUtil.writeObjectWithError(error, out);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        error = ErrorMessageUtil.readObjectWithError(in).orElse(null);
    }
}
