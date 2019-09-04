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
import java.util.Objects;

import org.apache.hyracks.api.util.ErrorMessageUtil;

public class HyracksException extends IOException implements IFormattedException {
    private static final long serialVersionUID = 1L;

    public static final int UNKNOWN = 0;
    private final String component;
    private final int errorCode;
    private final Serializable[] params;
    private final String nodeId;
    private SourceLocation sourceLoc;
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

    public static HyracksException create(int code, Serializable... params) {
        return new HyracksException(ErrorCode.HYRACKS, code, ErrorCode.getErrorMessage(code), params);
    }

    public static HyracksException create(int code, Throwable cause, Serializable... params) {
        return new HyracksException(ErrorCode.HYRACKS, code, ErrorCode.getErrorMessage(code), cause, params);
    }

    public HyracksException(String component, int errorCode, String message, Throwable cause, SourceLocation sourceLoc,
            String nodeId, Serializable... params) {
        super(message, cause);
        this.sourceLoc = sourceLoc;
        this.component = component;
        this.errorCode = errorCode;
        this.nodeId = nodeId;
        this.params = params;
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksException(String message) {
        this(ErrorMessageUtil.NONE, UNKNOWN, message, null, null, (Serializable[]) null);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    protected HyracksException(Throwable cause) {
        this(ErrorMessageUtil.NONE, UNKNOWN, ErrorMessageUtil.getCauseMessage(cause), cause, (Serializable[]) null);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    protected HyracksException(String message, Throwable cause) {
        this(ErrorMessageUtil.NONE, UNKNOWN, message, cause, (String) null);
    }

    public HyracksException(String component, int errorCode, Serializable... params) {
        this(component, errorCode, null, null, null, params);
    }

    public HyracksException(Throwable cause, int errorCode, Serializable... params) {
        this(ErrorMessageUtil.NONE, errorCode, ErrorMessageUtil.getCauseMessage(cause), cause, null, params);
    }

    public HyracksException(String component, int errorCode, String message, Serializable... params) {
        this(component, errorCode, message, null, null, params);
    }

    public HyracksException(String component, int errorCode, Throwable cause, Serializable... params) {
        this(component, errorCode, ErrorMessageUtil.getCauseMessage(cause), cause, null, params);
    }

    public HyracksException(String component, int errorCode, String message, Throwable cause, Serializable... params) {
        this(component, errorCode, message, cause, null, params);
    }

    public HyracksException(String component, int errorCode, String message, Throwable cause, String nodeId,
            Serializable... params) {
        this(component, errorCode, message, cause, null, nodeId, params);
    }

    @Override
    public String getComponent() {
        return component;
    }

    @Override
    public int getErrorCode() {
        return errorCode;
    }

    public Object[] getParams() {
        return params;
    }

    public String getNodeId() {
        return nodeId;
    }

    public SourceLocation getSourceLocation() {
        return sourceLoc;
    }

    public void setSourceLocation(SourceLocation sourceLocation) {
        this.sourceLoc = sourceLocation;
    }

    @Override
    public String getMessage() {
        if (msgCache == null) {
            msgCache = ErrorMessageUtil.formatMessage(component, errorCode, super.getMessage(), sourceLoc, params);
        }
        return msgCache;
    }

    public boolean matches(String component, int errorCode) {
        Objects.requireNonNull(component, "component");
        return component.equals(this.component) && errorCode == this.errorCode;
    }

    @Override
    public String toString() {
        return getLocalizedMessage();
    }
}
