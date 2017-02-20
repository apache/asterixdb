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

import org.apache.hyracks.api.util.ErrorMessageUtil;

public class HyracksException extends IOException {
    private static final long serialVersionUID = 1L;

    public static final int UNKNOWN = 0;
    private final String component;
    private final int errorCode;
    private final Serializable[] params;
    private final String nodeId;
    private transient volatile String msgCache;

    public static HyracksException create(int code, Serializable... params) {
        return new HyracksException(ErrorCode.HYRACKS, code, ErrorCode.getErrorMessage(code), params);
    }

    public static HyracksException create(int code, Throwable cause, Serializable... params) {
        return new HyracksException(ErrorCode.HYRACKS, code, ErrorCode.getErrorMessage(code), cause, params);
    }

    public HyracksException(String component, int errorCode, String message, Throwable cause, String nodeId,
            Serializable... params) {
        super(message, cause);
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
        this(ErrorMessageUtil.NONE, UNKNOWN, message, null, null);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksException(Throwable cause) {
        this(ErrorMessageUtil.NONE, UNKNOWN, cause.getMessage(), cause, null);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksException(Throwable cause, String nodeId) {
        this(ErrorMessageUtil.NONE, UNKNOWN, cause.getMessage(), cause, nodeId);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksException(String message, Throwable cause, String nodeId) {
        this(ErrorMessageUtil.NONE, UNKNOWN, message, cause, nodeId);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksException(String message, Throwable cause) {
        this(ErrorMessageUtil.NONE, UNKNOWN, message, cause, (String) null);
    }

    public HyracksException(String component, int errorCode, Serializable... params) {
        this(component, errorCode, null, null, null, params);
    }

    public HyracksException(Throwable cause, int errorCode, Serializable... params) {
        this(ErrorMessageUtil.NONE, errorCode, cause.getMessage(), cause, null, params);
    }

    public HyracksException(String component, int errorCode, String message, Serializable... params) {
        this(component, errorCode, message, null, null, params);
    }

    public HyracksException(String component, int errorCode, Throwable cause, Serializable... params) {
        this(component, errorCode, cause.getMessage(), cause, null, params);
    }

    public HyracksException(String component, int errorCode, String message, Throwable cause, Serializable... params) {
        this(component, errorCode, message, cause, null, params);
    }

    public String getComponent() {
        return component;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public Object[] getParams() {
        return params;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public String getMessage() {
        if (msgCache == null) {
            synchronized (this) {
                msgCache = ErrorMessageUtil.formatMessage(component, errorCode, super.getMessage(), params);
            }
        }
        return msgCache;
    }
}
