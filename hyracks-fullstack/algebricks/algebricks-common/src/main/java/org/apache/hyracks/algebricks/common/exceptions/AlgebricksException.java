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
package org.apache.hyracks.algebricks.common.exceptions;

import java.io.Serializable;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.util.ErrorMessageUtil;

public class AlgebricksException extends Exception implements IFormattedException {
    private static final long serialVersionUID = 1L;

    public static final int UNKNOWN = 0;
    private final String component;
    private final int errorCode;
    private final Serializable[] params;
    private final String nodeId;
    private final SourceLocation sourceLoc;

    @SuppressWarnings("squid:S1165") // exception class not final
    private transient CachedMessage msgCache;

    public AlgebricksException(String component, int errorCode, String message, Throwable cause,
            SourceLocation sourceLoc, String nodeId, Serializable... params) {
        super(message, cause);
        this.component = component;
        this.errorCode = errorCode;
        this.sourceLoc = sourceLoc;
        this.nodeId = nodeId;
        this.params = params;
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public AlgebricksException(String message) {
        this(ErrorMessageUtil.NONE, UNKNOWN, message, null, null, (Serializable[]) null);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public AlgebricksException(Throwable cause) {
        this(ErrorMessageUtil.NONE, UNKNOWN, cause.getMessage(), cause, (Serializable[]) null);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public AlgebricksException(String message, Throwable cause) {
        this(ErrorMessageUtil.NONE, UNKNOWN, message, cause, null, (Serializable[]) null);
    }

    public AlgebricksException(String component, int errorCode, SourceLocation sourceLoc, Serializable... params) {
        this(component, errorCode, null, null, sourceLoc, null, params);
    }

    public AlgebricksException(String component, int errorCode, Serializable... params) {
        this(component, errorCode, null, null, null, null, params);
    }

    public AlgebricksException(Throwable cause, int errorCode, SourceLocation sourceLoc, Serializable... params) {
        this(ErrorMessageUtil.NONE, errorCode, cause.getMessage(), cause, sourceLoc, null, params);
    }

    public AlgebricksException(Throwable cause, int errorCode, Serializable... params) {
        this(ErrorMessageUtil.NONE, errorCode, cause.getMessage(), cause, null, null, params);
    }

    public AlgebricksException(String component, int errorCode, String message, SourceLocation sourceLoc,
            Serializable... params) {
        this(component, errorCode, message, null, sourceLoc, null, params);
    }

    public AlgebricksException(String component, int errorCode, String message, Serializable... params) {
        this(component, errorCode, message, null, null, null, params);
    }

    public AlgebricksException(String component, int errorCode, Throwable cause, SourceLocation sourceLoc,
            Serializable... params) {
        this(component, errorCode, cause.getMessage(), cause, sourceLoc, null, params);
    }

    public AlgebricksException(String component, int errorCode, Throwable cause, Serializable... params) {
        this(component, errorCode, cause.getMessage(), cause, null, null, params);
    }

    public AlgebricksException(String component, int errorCode, String message, Throwable cause,
            SourceLocation sourceLoc, Serializable... params) {
        this(component, errorCode, message, cause, sourceLoc, null, params);
    }

    public AlgebricksException(String component, int errorCode, String message, Throwable cause,
            Serializable... params) {
        this(component, errorCode, message, cause, null, null, params);
    }

    public static AlgebricksException create(int errorCode, SourceLocation sourceLoc, Serializable... params) {
        return new AlgebricksException(ErrorCode.HYRACKS, errorCode, ErrorCode.getErrorMessage(errorCode), sourceLoc,
                params);
    }

    public static AlgebricksException create(int errorCode, Serializable... params) {
        return create(errorCode, null, params);
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

    @Override
    public String getMessage() {
        if (msgCache == null) {
            msgCache = new CachedMessage(
                    ErrorMessageUtil.formatMessage(component, errorCode, super.getMessage(), sourceLoc, params));
        }
        return msgCache.message;
    }

    private static class CachedMessage {
        private final String message;

        private CachedMessage(String message) {
            this.message = message;
        }
    }
}
